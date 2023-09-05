package mt.fireworks.associations;

import java.util.*;

/**
 * Container that maps stored values to each other based on their "similarity".
 * AssociationMap can have multiple data associations and for each it behaves
 * as form multimap. Using query data, all "similar" values can be get
 * from map.
 *
 * <p>After value is added to map, it is stored in multiple lists. Each list
 * contains data which is "similar" under some criteria, which is also set by
 * user as java {@code Function}.
 */
public interface AssociationMap<T> {


    /**
     * Add association function under keyName. Create new internal index
     * if one exists, or update existing with a new association function.
     *
     * <p>{@code association} function will for given value, added to map,
     * calculate key for internal index. Values which share same key,
     * are "associated" values. If {@code association} function returns
     * {@code null} no association is created.
     *
     * <p>One Association map can have multiple association functions
     * indexes as long as names differ.
     *
     * <p>Passing {@code null} will delete association / index or throw
     * exception, depending on needs of implementation. Removing index
     * will not delete data.
     *
     * <p>Implementation is free to ignore this method, or throw exception
     * if it is used in appropriate life cycle moment.
     *
     * @param keyName - name of internal index / association
     * @param assocation - function used to associate data.
     */
    // <K> void associate(String keyName, Function<T, K> association);


    /**
     * @return names of associations.
     */
    List<String> keys();


    /**
     * Adds object to this container. Then associates the specified value
     * to all similar values in this map. If no similar value is found,
     * a new entry is added to internal index of associated values, as long
     * as any association function returns non-null key.
     *
     * <p>Implementation can choose different handling of duplicates and
     * null values.
     *
     * @return true if object is added to cache
     */
    boolean add(T value);


    /**
     * Returns values which are associated to the query value under given
     * association name.
     *
     * @param keyName - name of internal index / association
     * @param query - object for which associated values are returned
     * @return data found to be associated to passed query under given index
     */
    List<T> get(String keyName, T query);


    /**
     * Similar to {@code #get(String, Object)} but returns associated data
     * for all indexes contained in map. Each association is stored in
     * returned map under key of given index.
     *
     * @param query - object for which associated values are returned
     * @return data associated to passed query, organizied in multiple list,
     *         one fore each index. Lists are stored under key names.
     */
    Map<String, List<T>> getAsMap(T query);


    /**
     * Try to add value to this AssociationMap. If successful return all
     * associations of passed value. Else return empty map.
     *
     * @param value to be added
     * @return associated values, or empty map.
     */
    default Map<String, List<T>> addAndGet(T value) {
        return add(value) ? getAsMap(value)
                          : Collections.emptyMap();
    }


    /**
     * Iterator of stored values in this map.
     */
    Iterator<T> values();


    /**
     * Return all values stored within index. Each invocation returns next value.
     */
    Iterator<T> indexValues(String keyName);


    /**
     * Return all values stored within index, grouped by associations.
     * Each invocation returns next associated group.
     */
    Iterator<List<T>> indexAssociations(String keyName);

}
