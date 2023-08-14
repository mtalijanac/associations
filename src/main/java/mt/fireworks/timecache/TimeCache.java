package mt.fireworks.timecache;

import java.util.*;

import org.eclipse.collections.impl.map.mutable.UnifiedMap;

public interface TimeCache<T, K> {

    /**
     * @return true if object is added to cache
     */
    boolean add(T val);


    /**
     * @return entries associated to given value
     */
    List<CacheEntry<K, List<T>>> get(T val);


    default Map<String, List<T>> getAsMap(T val) {
        List<CacheEntry<K, List<T>>> list = get(val);
        Map<String, List<T>> res = UnifiedMap.newMap(list.size() + 1, 1f);
        for (CacheEntry<K, List<T>> ce : list) {
            String name = ce.getName();
            List<T> value = ce.getValue();
            res.put(name, value);
        }
        return res;
    }


    /**
     * @return associated entries to given value which are older than given tstamp
     */
    default List<CacheEntry<K, List<T>>> getOlder(T val, long tstamp) {
        throw new UnsupportedOperationException();
    }


    /**
     * First add a value than return associated entries.
     * If data was not added, than no associated entries are fetched.
     *
     * @see #add(Object)
     * @see #get(Object)
     */
    default List<CacheEntry<K, List<T>>> addAndGet(T val) {
        return add(val) ? get(val) : Collections.emptyList();
    }


    /** move caching window to next iteration, and gc old entries */
    void tick();


    /** @return start time of current window */
    long startTimeMillis();

}
