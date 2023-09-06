package mt.fireworks.associations;

import java.util.List;
import java.util.Map;

public interface AssociationCache<T> extends AssociationMap<T> {


    /** move caching window to next iteration, and gc old entries */
    void tick();


    /** @return start time of current window */
    long startTimeMillis();


    List<T> get(String indexName, T query, Long fromInclusive, Long toExclusive);


    Map<String, List<T>> getAsMap(T query, Long fromInclusive, Long toExclusive);

}
