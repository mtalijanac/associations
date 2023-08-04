package mt.fireworks.timecache;

import java.util.Arrays;
import java.util.function.Function;

import org.eclipse.collections.api.block.HashingStrategy;
import org.eclipse.collections.api.collection.primitive.MutableLongCollection;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.map.strategy.mutable.UnifiedMapWithHashingStrategy;

import lombok.Data;

@Data
public class Index<T> {

    MutableMap<byte[], MutableLongCollection> index;
    Function<T, byte[]> keyer;
    TimeKeys timeKeys;



    Index(Function<T, byte[]> keyer, TimeKeys tk) {
        this.keyer = keyer;
        this.timeKeys = tk;

        UnifiedMapWithHashingStrategy<byte[], MutableLongCollection> map = new UnifiedMapWithHashingStrategy<>(new IndexHashCode());
        index = map.asSynchronized();
    }


    public boolean put(T val, long storageKey) {
        byte[] key = keyer.apply(val);
        if (key == null) return false;
        MutableLongCollection keyData = index.getIfAbsentPut(key, () -> LongLists.mutable.empty().asSynchronized());
        keyData.add(storageKey);
        return true;
    }

    public MutableLongCollection get(T val) {
        byte[] key = keyer.apply(val);
        if (key == null) return null;
        MutableLongCollection keyData = index.get(key);
        return keyData;
    }


    /**
     * Values of T which happened on same tstamp by {@link TimeKeys#equalSec(long, long)}
     */
    public MutableLongCollection onSameTime(T val, long valTstamp) {
        byte[] valKey = keyer.apply(val);
        if (valKey == null) return null;
        MutableLongCollection keyData = index.get(valKey);
        if (keyData == null) return null;

        boolean matching = keyData.anySatisfy(storedKey -> {
            long keyTstamp = timeKeys.tstamp(storedKey);
            boolean sameTime = timeKeys.equalSec(valTstamp, keyTstamp);
            return sameTime;
        });

        if (!matching) return null;

        MutableLongCollection onSameTime = keyData.select(storedKey -> {
            long keyTstamp = timeKeys.tstamp(storedKey);
            boolean sameTime = timeKeys.equalSec(valTstamp, keyTstamp);
            return sameTime;
        });

        return onSameTime;

    }

    public void gc(long upperTstampExclusive) {
        // TODO ukloni prazne ključeve

        MutableLongList tmpBuffer = LongLists.mutable.empty();

        index.forEachKeyValue((key, values) -> {
            if (values == null) return;
            if (values.size() == 0) return;

            values.forEach(storageKey -> {
                long tstamp = timeKeys.tstamp(storageKey);
                if (tstamp < upperTstampExclusive)
                    tmpBuffer.add(storageKey);
            });

            if (tmpBuffer.size() == 0) return;

            values.removeAll(tmpBuffer);
            tmpBuffer.clear();
        });
    }

    public void clearKey(T val, long upperTstampExclusive) {
        // TODO ukloni prazne ključeve

        byte[] key = keyer.apply(val);
        if (key == null) return;

        MutableLongCollection keyData = index.get(key);
        if (keyData == null) return;
        if (keyData.isEmpty()) return;

        MutableLongList tmpBuffer = LongLists.mutable.empty();

        keyData.forEach(storageKey -> {
            long tstamp = timeKeys.tstamp(storageKey);
            if (tstamp < upperTstampExclusive) {
                tmpBuffer.add(storageKey);
            }
        });

        if (tmpBuffer.size() == 0) return;
        keyData.removeAll(tmpBuffer);
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
