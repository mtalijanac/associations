package mt.fireworks.timecache;

import java.util.*;
import java.util.Map.Entry;

public interface TimeCache<T, K> {

    /**
     * @return true if object is added to cache
     */
    boolean add(T val);


    /**
     * @return entries associated to given value
     */
    List<Entry<K, List<T>>> get(T val);


    /**
     * @return associated entries to given value which are older than given tstamp
     */
    default List<Entry<K, List<T>>> getOlder(T val, long tstamp) {
        throw new UnsupportedOperationException();
    }


    /**
     * First add a value than return associated entries.
     * If data was not added, than no associated entries are fetched.
     *
     * @see #add(Object)
     * @see #get(Object)
     */
    default List<Entry<K, List<T>>> addAndGet(T val) {
        return add(val) ? get(val) : Collections.emptyList();
    }


    /** move caching window to next iteration, and gc old entries */
    void tick();

}
