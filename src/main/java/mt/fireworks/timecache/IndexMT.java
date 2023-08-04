package mt.fireworks.timecache;

import java.util.Arrays;
import java.util.function.Function;

import org.eclipse.collections.api.block.HashingStrategy;
import org.eclipse.collections.api.collection.primitive.MutableLongCollection;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.map.strategy.mutable.UnifiedMapWithHashingStrategy;

import lombok.Data;

@Data
class IndexMT<T> {

    MutableMap<byte[], MutableLongCollection>[] indexes = new MutableMap[32];
    Function<T, byte[]> keyer;

    {
        for (int idx = 0; idx < indexes.length; idx++) {
            UnifiedMapWithHashingStrategy<byte[], MutableLongCollection> map = new UnifiedMapWithHashingStrategy<>(new IndexHashCode());
            indexes[idx] = map.asSynchronized();
        }
    }

    public MutableMap<byte[], MutableLongCollection> index(byte[] a) {
        if (a == null) return indexes[0];

        int result = 1;
        for (int idx = 0; idx < a.length; idx++) {
            byte element = a[idx];
            result = 31 * result + element;
        }

        int index = Math.abs(result) % indexes.length;
        MutableMap<byte[], MutableLongCollection> map = indexes[index];
        return map;
    }

    public boolean put(T val, long storageKey) {
        byte[] key = keyer.apply(val);
        if (key == null) return false;

        MutableLongCollection keyData = index(key).getIfAbsentPut(key, () -> LongLists.mutable.empty().asSynchronized());
        keyData.add(storageKey);
        return true;
    }

    public MutableLongCollection get(T val) {
        byte[] key = keyer.apply(val);
        if (key == null) return null;
        MutableLongCollection keyData = index(key).get(key);
        return keyData;
    }

    public void gc(long upperTstampExclusive) {
        // TODO ukloni prazne ključeve

        MutableLongCollection tmpBuffer = LongLists.mutable.empty();

        for (int idx = 0; idx < indexes.length; idx++) {
            MutableMap<byte[], MutableLongCollection> index = indexes[idx];
            index.forEachKeyValue((key, values) -> {
                if (values == null) return;
                if (values.size() == 0) return;

                values.forEach(storageKey -> {
                    if (storageKey < upperTstampExclusive)
                        tmpBuffer.add(storageKey);
                });

                if (tmpBuffer.size() == 0) return;

                values.removeAll(tmpBuffer);
                tmpBuffer.clear();
            });
        }
    }


    static class IndexHashCode implements HashingStrategy<byte[]> {
        @Override
        public int computeHashCode(byte[] a) {
            if (a == null) return 0;

            int result = 1;
            for (int idx = 0; idx < a.length; idx++) {
                byte element = a[idx];
                result = 31 * result + element;
            }

            return result;
        }

        @Override
        public boolean equals(byte[] object1, byte[] object2) {
            return Arrays.equals(object1, object2);
        }
    }

}
