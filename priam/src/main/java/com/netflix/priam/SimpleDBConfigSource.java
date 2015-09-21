package com.netflix.priam;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.netflix.priam.identity.PriamInstance;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;

/**
 * Loads config data from SimpleDB.  {@link #intialize(String, String)} will query the SimpleDB domain "PriamProperties"
 * for any potential configurations.  The domain is set up to support multiple different clusters; this is done by using
 * amazon's auto scaling groups (ASG).
 * <p/>
 * Schema <ul>
 *   <li>"appId" // ASG up to first instance of '-'.  So ASG name priam-test will create appId priam, ASG priam_test
 *   will create appId priam_test.</li>
 *   <li>"property" // key to use for configs.</li>
 *   <li>"value" // value to set for the given property/key.</li>
 *   <li>"region" // region the config belongs to.  If left empty, then applies to all regions.</li>
 * </ul> }
 */
public final class SimpleDBConfigSource extends AbstractConfigSource 
{
    private static final Logger logger = LoggerFactory.getLogger(SimpleDBConfigSource.class.getName());

    private static final String DOMAIN = "PriamProperties";
    private static String ALL_QUERY = "select * from " + DOMAIN + " where " + Attributes.APP_ID + "='%s'";

    private final Map<String, String> data = Maps.newConcurrentMap();
    private final ICredential provider;

    private String appid;

    @Inject
    public SimpleDBConfigSource(final ICredential provider) 
    {
        this.provider = provider;
    }

    @Override
    public void intialize(final String asgName, final String region) 
    {
        super.intialize(asgName, region);

        // End point is us-east-1
        AmazonSimpleDBClient simpleDBClient = new AmazonSimpleDBClient(provider.getAwsCredentialProvider());

        String nextToken = null;
        this.appid = asgName.lastIndexOf('-') > 0 ? asgName.substring(0, asgName.indexOf('-')) : asgName;
        logger.info(String.format("appid used to fetch properties is: %s", appid));
        do 
        {
            SelectRequest request = new SelectRequest(String.format(ALL_QUERY, appid));
            request.setNextToken(nextToken);
            SelectResult result = simpleDBClient.select(request);
            nextToken = result.getNextToken();
            Iterator<Item> itemiter = result.getItems().iterator();
            while (itemiter.hasNext())
              addProperty(itemiter.next());

        } 
        while (nextToken != null);
    }

    private static class Attributes 
    {
        public final static String APP_ID = "appId"; // ASG or first part thereof
        public final static String PROPERTY = "property";
        public final static String PROPERTY_VALUE = "value";
        public final static String REGION = "region";
    }

    private void addProperty(Item item) 
    {
        Iterator<Attribute> attrs = item.getAttributes().iterator();
        String prop = "";
        String value = "";
        String dc = "";
        while (attrs.hasNext()) 
        {
            Attribute att = attrs.next();
            if (att.getName().equals(Attributes.PROPERTY))
                prop = att.getValue();
            else if (att.getName().equals(Attributes.PROPERTY_VALUE))
                value = att.getValue();
            else if (att.getName().equals(Attributes.REGION))
                dc = att.getValue();
        }
        // Ignore, if not this region
        if (StringUtils.isNotBlank(dc) && !dc.equals(getRegion()))
            return;
        // Override only if region is specified
        if (data.containsKey(prop) && StringUtils.isBlank(dc))
            return;
        logger.debug("Adding property {} with value {}",prop,value);
        data.put(prop, value);
    }

    @Override
    public int size() 
    {
        return data.size();
    }

    @Override
    public String get(final String key) 
    {
        return data.get(key);
    }

    @Override
    public void set(final String key, final String value)
    {
        Preconditions.checkNotNull(value, "Value can not be null for configurations.");
        logger.debug("Setting property {} with value {}", key, value);
        data.put(key, value);
    }
    public Set<String> keySet() {
        return data.keySet();
    }
    public void save() {
        // create a put request with attributes
        List<ReplaceableAttribute> attrs = new ArrayList<ReplaceableAttribute>();
        for (String key : data.keySet()) {
            String value = data.get(key);
            if (value != null && !value.isEmpty())
                attrs.add(new ReplaceableAttribute(key, value, false)); // lazy way of making sure we don't overwrite if this was all some kind of mistake

        }
        try {
            AmazonSimpleDBClient simpleDBClient = new AmazonSimpleDBClient(provider.getAwsCredentialProvider());
            PutAttributesRequest putReq = new PutAttributesRequest(DOMAIN, appid, attrs);
            simpleDBClient.putAttributes(putReq);
        }
        catch (AmazonServiceException ase) {
            // log msg about how sorry i am
        }
    }
}
