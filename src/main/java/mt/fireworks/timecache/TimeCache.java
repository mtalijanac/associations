package mt.fireworks.timecache;

import java.util.*;

public interface TimeCache<T> {

    /**
     * @return true if object is added to cache
     */
    boolean add(T val);


    List<T> get(String indexName, T query, Long fromInclusive, Long toExclusive);


    default List<T> get(String indexName, T query) {
        return get(indexName, query, null, null);
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
    Map<String /* INDEX NAME */, List<T>> getAsMap(T val, Long fromInclusive, Long toExclusive);


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
    default Map<String, List<T>> addAndGet(T val) {
        return add(val) ? getAsMap(val) : Collections.emptyMap();
    }


    /** move caching window to next iteration, and gc old entries */
    void tick();


    /** @return start time of current window */
    long startTimeMillis();

}
