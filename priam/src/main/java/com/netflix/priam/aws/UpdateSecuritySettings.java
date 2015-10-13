/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.aws;

import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;

/**
 * this class will associate an Public IP's with a new instance so they can talk
 * across the regions.
 * 
 * Requirement: 1) Nodes in the same region needs to be able to talk to each
 * other. 2) Nodes in other regions needs to be able to talk to the others in
 * the other region.
 * 
 * Assumption: 1) IPriamInstanceFactory will provide the membership... and will
 * be visible across the regions 2) IMembership amazon or any other
 * implementation which can tell if the instance is part of the group (ASG in
 * amazons case).
 * 
 */
@Singleton
public class UpdateSecuritySettings extends Task
{
    public static final String JOBNAME = "Update_SG";
    public static boolean firstTimeUpdated = false;

    private static final Random ran = new Random();
    private final IMembership membership;
    private final IPriamInstanceFactory<PriamInstance> factory;

    @Inject
    //Note: do not parameterized the generic type variable to an implementation as it confuses Guice in the binding.
    public UpdateSecuritySettings(IConfiguration config, IMembership membership, IPriamInstanceFactory factory)
    {
        super(config);
        this.membership = membership;
        this.factory = factory;
    }

    /**
     * Seeds nodes execute this at the specifed interval.
     * Other nodes run only on startup.
     * Seeds in cassandra are the first node in each Availablity Zone.
     */
    @Override
    public void execute()
    {
        // acl range is a cidr, but in this case /32 is appended to make single IP a "range"
        int fPort = config.getStoragePort();
        int tPort = config.getSSLStoragePort();
        List<String> acls = membership.listACL(fPort, tPort); // list of all the current ranges in SG
        List<PriamInstance> allInstances = factory.getAllIds(config.getAppName()); // All instance Priam knows about
        List<String> currentRanges = Lists.newArrayList(); // take the list of instances and get list of ranges
        List<String> add = Lists.newArrayList(); // list of ranges to add to SG
        List<String> remove = Lists.newArrayList(); // list of ranges to remove from SG

        // iterate to add...
        // first time through, add my ip to the sg
        if (status != STATE.STOPPING && !firstTimeUpdated) {
            String range = config.getHostIP() + "/32"; // range for this instance
            if (!acls.contains(range))
                add.add(range);
            firstTimeUpdated = true;
        }

        for (PriamInstance instance : allInstances)
        {
            String range = instance.getHostIP() + "/32";
            currentRanges.add(range); // list of ips priam knows about
            // only add hosts from other DCs, hosts in this region should be managing themselves
            if (instance.getDC() != config.getDC() && !acls.contains(range)) {
                add.add(range);
            }
        }

        // iterate to remove...
        for (String acl : acls) {
            if (!currentRanges.contains(acl)) // if not found then remove....
                remove.add(acl);
        }
        if (status == STATE.STOPPING) remove.add(config.getHostIP() + "/32");
        if (add.size() > 0)
        {
            membership.addACL(add, fPort, tPort);
        }
        if (remove.size() > 0)
        {
            membership.removeACL(remove, fPort, tPort);
        }
    }

    public static TaskTimer getTimer(InstanceIdentity id)
    {
        SimpleTimer return_;
        if (id.isSeed())
            return_ = new SimpleTimer(JOBNAME, 120 * 1000 + ran.nextInt(120 * 1000));
        else
            return_ = new SimpleTimer(JOBNAME);
        return return_;
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }
}
