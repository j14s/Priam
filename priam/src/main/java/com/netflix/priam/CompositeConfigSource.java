package com.netflix.priam;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * A {@link IConfigSource} that delegates method calls to the underline sources.  The order in which values are provided
 * depend on the {@link IConfigSource}s provided.  If user asks for key 'foo', and this composite has three sources, it
 * will first check if the key is found in the first source, if not it will check the second and if not, the third, else
 * return null or false if {@link #contains(String)} was called.
 * <p/>
 * Implementation note: get methods with a default are implemented in {@link AbstractConfigSource}, if the underlying
 * source overrides one of these methods, then that implementation will be ignored.
 */
public class CompositeConfigSource extends AbstractConfigSource 
{
    private static final Logger logger = LoggerFactory.getLogger(CompositeConfigSource.class);

    private final ImmutableCollection<? extends IConfigSource> sources;

    public CompositeConfigSource(final ImmutableCollection<? extends IConfigSource> sources) 
    {
        Preconditions.checkArgument(!sources.isEmpty(), "Can not create a composite config source without config sources!");
        this.sources = sources;
    }

    public CompositeConfigSource(final Collection<? extends IConfigSource> sources) 
    {
        this(ImmutableList.copyOf(sources));
    }

    public CompositeConfigSource(final Iterable<? extends IConfigSource> sources) 
    {
        this(ImmutableList.copyOf(sources));
    }

    public CompositeConfigSource(final IConfigSource... sources) 
    {
        this(ImmutableList.copyOf(sources));
    }

    public void rollup()
    {
        HashMap<String,String> allData = new HashMap<String, String>();
        // start at top, make list of all items in source
        for (final IConfigSource source : sources)
        {
            Iterator<String> i  = source.keySet().iterator();
            while (i.hasNext()) {
                String key = i.next();
                // go down to other sources, only use value if it isn't in the list already
                if (!allData.containsKey(key)) {
                    String value = source.get(key);
                    if (value != null && !value.isEmpty()) {
                        allData.put(key, value);
                    }
                }
            }
        }
        // end with set back to first list and then call to that sources save method
        Iterator<String> i  = allData.keySet().iterator();
        while (i.hasNext()) {
            String key  = i.next();
            this.set(key,allData.get(key));
        }

    }
    @Override
    public void intialize(final String asgName, final String region) 
    {
        for (final IConfigSource source : sources) 
        {
            //TODO should this catch any potential exceptions?
            source.intialize(asgName, region);
        }
    }

    @Override
    public int size() 
    {
        int size = 0;
        for (final IConfigSource c : sources) 
        {
            size += c.size();
        }
        return size;
    }

    @Override
    public boolean isEmpty() 
    {
        return size() == 0;
    }

    @Override
    public boolean contains(final String key) 
    {
        return get(key) != null;
    }

    @Override
    public String get(final String key) 
    {
        logger.info("Getting value for:" + key);
        Preconditions.checkNotNull(key);
        for (final IConfigSource c : sources) 
        {
            final String value = c.get(key);
            logger.info(c.toString() + "has value :" + value);
            if (value != null)
            {
                return value;
            }
        }
        return null;
    }

    @Override
    public void set(final String key, final String value)
    {
        Preconditions.checkNotNull(value, "Value can not be null for configurations.");
        final IConfigSource firstSource = Iterables.getFirst(sources, null);
        // firstSource shouldn't be null because the collection is immutable, and the collection is non empty.
        Preconditions.checkState(firstSource != null, "There was no IConfigSource found at the first location?");
        firstSource.set(key, value);
    }

    @Override
    public void save() {
        try {
            final IConfigSource firstSource = Iterables.getFirst(sources, null);
            // firstSource shouldn't be null because the collection is immutable, and the collection is non empty.
            Preconditions.checkState(firstSource != null, "There was no IConfigSource found at the first location?");
            firstSource.save();
        } catch (UnsupportedOperationException oue) {
            logger.warn("Failed to save final configuration.", oue);
        }
    }
    @Override
    public Set<String> keySet() {
        // unimplemented
        return null;
    }
}
