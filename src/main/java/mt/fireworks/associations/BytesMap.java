package mt.fireworks.associations;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.map.strategy.mutable.UnifiedMapWithHashingStrategy;

import lombok.*;
import mt.fireworks.associations.BytesMapBuilder.AddSerdes;
import mt.fireworks.associations.BytesMapBuilder.Builder;

public class BytesMap<T> implements AssociationMap<T> {

    final SerDes<T> serdes;
    final ArrayList<Index<T>> indexes = new ArrayList<>();
    final ByteList byteList;
    final List<String> keys;


    @Data @RequiredArgsConstructor
    static class Index<T> {
        @NonNull String name;
        @NonNull Function<T, byte[]> keyer;

        MutableMap<byte[], MutableLongList> map = new UnifiedMapWithHashingStrategy<>(new BytesHashingStrategy());
    }

    public static <T> AddSerdes<T> newInstance(Class<T> klazz) {
        return new Builder<>();
    }


    BytesMap(SerDes<T> serdes, ArrayList<Index<T>> indexes, Integer allocationSize) {
        this.serdes = serdes;
        this.indexes.addAll(indexes);
        this.keys = indexes.stream().map(Index::getName).collect(Collectors.toUnmodifiableList());
        this.byteList = new ByteList(allocationSize);
    }


    @Override
    public List<String> keys() {
        return keys;
    }


    @Override
    public boolean add(T val) {
        byte[] data = null;
        long storageKey = 0;

        for (int idx = 0; idx < indexes.size(); idx++) {
            Index<T> index = indexes.get(idx);
            byte[] key = index.keyer.apply(val);
            if (key == null) continue;
            if (data == null) {
                data = serdes.marshall(val);
                storageKey = byteList.add(data);
            }

            MutableLongList keyData = index.map.getIfAbsentPut(key, () -> LongLists.mutable.withInitialCapacity(1));
            keyData.add(storageKey);
        }

        return data != null;
    }


    @Override
    public List<T> get(String indexName, T query) {
        Index<T> index = index(indexName);
        if (index == null) return Collections.emptyList();
        List<T> res = readIndex(index, query);
        return res;
    }


    Index<T> index(String indexName) {
        for (int i = 0; i < indexes.size(); i++) {
            Index<T> index = indexes.get(i);
            if (indexName.equals(index.getName())) {
                return index;
            }
        }
        return null;
    }


    @Override
    public Map<String, List<T>> getAsMap(T query) {
        int capacity = Math.round(indexes.size() / 0.75f) + 1;
        UnifiedMap<String, List<T>> result = new UnifiedMap<>(capacity);

        for (int i = 0; i < indexes.size(); i++) {
            Index<T> index = indexes.get(i);
            List<T> res = readIndex(index, query);
            result.put(index.getName(), res);
        }
        return result;
    }


    @Override
    public Map<String, List<T>> addAndGet(T val) {
        return add(val) ? getAsMap(val) : Collections.emptyMap();
    }


    List<T> readIndex(Index<T> index, T query) {
        byte[] key = index.getKeyer().apply(query);
        if (key == null) return Collections.emptyList();

        MutableLongList strKeys = index.map.get(key);
        if (strKeys == null)   return Collections.emptyList();
        if (strKeys.isEmpty()) return Collections.emptyList();

        MutableLongList keysForRemoval = null;
        ArrayList<T> result = new ArrayList<>(strKeys.size());

        for (int jdx = 0; jdx < strKeys.size(); jdx++) {
            long strKey = strKeys.get(jdx);
            byte[] data = byteList.get(strKey);
            T res = serdes.unmarshall(data);
            if (res == null) {
                if (keysForRemoval == null) {
                    keysForRemoval = LongLists.mutable.empty();
                }
                keysForRemoval.add(strKey);
                continue;
            }
            result.add(res);
        }

        if (keysForRemoval != null && keysForRemoval.size() > 0) {
            strKeys.removeAll(keysForRemoval);
            keysForRemoval.clear();
        }

        return result;
    }


    /**
     * Iterator of stored values in this map.
     */
    @Override
    public Iterator<T> values() {
        return byteList.iterator((objPos, bucket, pos, len) -> serdes.unmarshall(bucket, pos, len));
    }

    /**
     * Return all values stored within index, grouped by associations.
     * Each invocation returns next associated group.
     */
    @Override
    public Iterator<List<T>> indexAssociations(String keyName) {
        Index<T> index = index(keyName);
        Collection<MutableLongList> values = index.map.values();
        Iterator<MutableLongList> valuesIter = values.iterator();

        Iterator<List<T>> result = new Iterator<List<T>>() {
            public boolean hasNext() {
                return valuesIter.hasNext();
            }

            @Override
            public List<T> next() {
                MutableLongList strKeys = valuesIter.next();
                if (strKeys == null)   return Collections.emptyList();
                if (strKeys.isEmpty()) return Collections.emptyList();

                ArrayList<T> result = new ArrayList<>(strKeys.size());
                for (int jdx = 0; jdx < strKeys.size(); jdx++) {
                    long strKey = strKeys.get(jdx);
                    byte[] data = byteList.get(strKey);
                    T res = serdes.unmarshall(data);
                    if (res == null) {
                        continue;
                    }
                    result.add(res);
                }

                return result;
            }
        };

        return result;
    }


    /**
     * Return all values stored within index. Each invocation returns next value.
     */
    @Override
    public Iterator<T> indexValues(String keyName) {
        return new Iterator<T>() {
            Iterator<List<T>> indexAssociations = indexAssociations(keyName);
            Iterator<T> association;

            public boolean hasNext() {
                if (association == null && !indexAssociations.hasNext()) {
                    return false;
                }

                if (association != null && association.hasNext()) {
                    return true;
                }

                while (indexAssociations.hasNext()) {
                    association = indexAssociations.next().iterator();
                    if (association.hasNext()) return true;
                }

                return false;
            }

            public T next() {
                return association.next();
            }
        };
    }

}
