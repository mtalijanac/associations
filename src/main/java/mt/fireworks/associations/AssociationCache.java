package mt.fireworks.associations;

import java.util.List;
import java.util.Map;

public interface AssociationCache<T> extends AssociationMap<T> {


    /** move caching window to next iteration, and gc old entries */
    void tick();


    /** @return start time of current window */
    long startTimeMillis();


    /**
     * Get stored objects under given index, within given timeperiod.
     * Time limits are optional and only obeyd when provided.
     *
     * @return objects matching query, within given index and time period.
     */
    List<T> get(String indexName, T query, Long fromInclusive, Long toExclusive);

    /** Same as get, but fetches only {@code count} newest objects within given range. */
    List<T> getLast(String indexName, T query, Integer count, Long fromInclusive, Long toExclusive);

    
    /**
     * Get objects matching query, within given index and time period.
     * All indexes are searched, but entry will be present in result
     * for only those indexes which contain at least one matchin object.
     *
     * @return Map of index name, and list of matching objects under that index. Never null.
     */
    Map<String /*index name*/, List<T>> getAsMap(T query, Long fromInclusive, Long toExclusive);
    
    
    /** Same ase {@link #getAsMap(Object)} but only index matching <code>indexNames</code> will be searched. */
    Map<String, List<T>> getByName(T query, Long fromInclusive, Long toExclusive, String... indexNames);
    
    /**
     * Same as {@link #getByName(Object, Long, Long, String...)} but only one index is search.
     * Avoids allocating intermediate object for varargs
     */
    Map<String, List<T>> getByName(T query, Long fromInclusive, Long toExclusive, String indexName);
    
    /**
     * Same as {@link #getByName(Object, Long, Long, String...)} but only two indexes are searched.
     * Avoids allocating intermediate object for varargs
     */
    Map<String, List<T>> getByName(T query, Long fromInclusive, Long toExclusive, String indexName1, String indexName2);
    
    /**
     * Same as {@link #getByName(Object, Long, Long, String...)} but only three indexes are searched.
     * Avoids allocating intermediate object for varargs
     */
    Map<String, List<T>> getByName(T query, Long fromInclusive, Long toExclusive, String indexName1, String indexName2, String indexName3);

}


