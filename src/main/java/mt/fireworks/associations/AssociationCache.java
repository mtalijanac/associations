package mt.fireworks.associations;

import java.util.List;
import java.util.Map;

public interface AssociationCache<T> extends AssociationMap<T> {


    /** move caching window to next iteration, and gc old entries */
    void tick();


    /** @return start time of current window */
    long startTimeMillis();


    List<T> get(String indexName, T query, Long fromInclusive, Long toExclusive);

    /** Same as get, but fetches only {@code count} last objects. */
    List<T> getLast(String indexName, T query, Integer count, Long fromInclusive, Long toExclusive);

    Map<String, List<T>> getAsMap(T query, Long fromInclusive, Long toExclusive);

}
