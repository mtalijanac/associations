package mt.fireworks.timecache.storage;

import java.util.Arrays;
import java.util.function.Function;

import org.eclipse.collections.api.block.HashingStrategy;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.map.strategy.mutable.UnifiedMapWithHashingStrategy;

import lombok.Data;

@Data
public class IndexMT<T> {

    MutableMap<byte[], MutableLongSet>[] indexes = new MutableMap[32];
    Function<T, byte[]> keyer;

    {
        for (int idx = 0; idx < indexes.length; idx++) {
            UnifiedMapWithHashingStrategy<byte[], MutableLongSet> map = new UnifiedMapWithHashingStrategy<>(new IndexHashCode());
            indexes[idx] = map.asSynchronized();
        }
    }

    public MutableMap<byte[], MutableLongSet> index(byte[] a) {
        if (a == null) return indexes[0];

        int result = 1;
        for (int idx = 0; idx < a.length; idx++) {
            byte element = a[idx];
            result = 31 * result + element;
        }

        int index = Math.abs(result) % indexes.length;
        MutableMap<byte[], MutableLongSet> map = indexes[index];
        return map;
    }

    public boolean put(T val, long storageKey) {
        byte[] key = keyer.apply(val);
        if (key == null) return false;

        MutableLongSet keyData = index(key).getIfAbsentPut(key, () -> LongSets.mutable.empty().asSynchronized());
        keyData.add(storageKey);
        return true;
    }

    public MutableLongSet get(T val) {
        byte[] key = keyer.apply(val);
        if (key == null) return null;
        MutableLongSet keyData = index(key).get(key);
        return keyData;
    }

    public void gc(long upperTstampExclusive) {
        // TODO ukloni prazne ključeve

        MutableLongList tmpBuffer = LongLists.mutable.empty();

        for (int idx = 0; idx < indexes.length; idx++) {
            MutableMap<byte[], MutableLongSet> index = indexes[idx];
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
