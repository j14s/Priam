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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    protected static final Logger logger = LoggerFactory.getLogger(UpdateSecuritySettings.class);
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
            String range = config.getHostname() + "/32"; // add the private IP
            logger.info("This is my first time.. adding private range for this instance:" + range);
            if (!acls.contains(range))
                add.add(range);
            range = config.getHostIP() + "/32"; // add the public address
            logger.info("This is my first time.. adding publi range for this instance:" + range);
            if (!acls.contains(range))
                add.add(range);
            firstTimeUpdated = true;
        }

        // build list of current ranges we _should_ have in the SG
        // plus, add ranges from other regions
        for (PriamInstance instance : allInstances)
        {
            logger.trace("config.getDC=" + config.getDC());
            logger.trace("instance.getDC=" + instance.getDC());
            String range;
            if (instance.getDC().equals(config.getDC()))
                currentRanges.add( instance.getHostName() + "/32" ); // private

            range = instance.getHostIP() + "/32";
            currentRanges.add(range); // public

            logger.debug("Grokking other ranges; found:" + range);
            // if my host, add to current, but not to add (because it was done above)
            if (config.getHostname().equals(instance.getHostName())) continue;
            // only add hosts from other DCs, hosts in this region should be managing themselves
            if (!acls.contains(range)) {
                add.add(range);
                logger.debug(range + " was not found in current ACL.");
            }
        }

        // iterate to remove...
        for (String acl : acls) {
            if (!currentRanges.contains(acl)) {// if not found then remove....
                remove.add(acl);
                logger.debug(acl + " is being removed from ACL because I didn't find a matching instance.");
            }
        }
        if (status == STATE.STOPPING) {
            remove.add(config.getHostname() + "/32");
            remove.add(config.getHostIP() + "/32");
        }
        if (remove.size() > 0)
        {
            membership.removeACL(remove, fPort, tPort);
        }
        if (add.size() > 0)
        {
            membership.addACL(add, fPort, tPort);
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
