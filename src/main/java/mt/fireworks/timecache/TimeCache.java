package mt.fireworks.timecache;

import java.util.*;

import org.eclipse.collections.impl.map.mutable.UnifiedMap;

public interface TimeCache<T, K> {

    /**
     * @return true if object is added to cache
     */
    boolean add(T val);


    /**
     * @param val
     * @param fromInclusive - lower tstamp limit, inclusive, nullable
     * @param toExclusive - upper tstamp limit, exclusive, nullable
     * @return entries associated to given value, filtered by tstamp
     */
    List<CacheEntry<K, List<T>>> get(T val, Long fromInclusive, Long toExclusive);

    default List<T> get(String index, T val, Long fromInclusive, Long toExclusive) {
        Map<String, List<T>> map = getAsMap(val, fromInclusive, toExclusive);
        List<T> res = map.get(index);
        return res;
    }


    /**
     * @return entries associated to given value
     */
    default List<CacheEntry<K, List<T>>> get(T val) {
        return get(val, null, null);
    }

    /**
     * As get, but return value is map. Keys are index names.
     * Map values are associated entries.
     *
     * @param val
     * @param fromInclusive - lower tstamp limit, inclusive, nullable
     * @param toExclusive - upper tstamp limit, exclusive, nullable
     * @return map of associated entries associated values, filtered by tstamp,
     * @see #get(Object, Long, Long)
     */
    default Map<String /* INDEX NAME */, List<T>> getAsMap(T val, Long fromInclusive, Long toExclusive) {
        List<CacheEntry<K, List<T>>> list = get(val, fromInclusive, toExclusive);
        Map<String, List<T>> res = UnifiedMap.newMap(list.size() + 1, 1f);
        for (CacheEntry<K, List<T>> ce : list) {
            String name = ce.getName();
            List<T> value = ce.getValue();
            res.put(name, value);
        }
        return res;
    }

    default Map<String, List<T>> getAsMap(T val) {
        return getAsMap(val, null, null);
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
