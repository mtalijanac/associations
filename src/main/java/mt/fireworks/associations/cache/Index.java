package mt.fireworks.associations.cache;

import static mt.fireworks.associations.cache.TimeUtils.info;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.eclipse.collections.api.block.HashingStrategy;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.map.strategy.mutable.UnifiedMapWithHashingStrategy;

import lombok.*;
import mt.fireworks.associations.Associations;

@Data
class Index<T> {

    final static HashingStrategy<byte[]> bytesHashing = Associations.bytesHashingStrategy();


    /** index name */
    String name;

    /** function which maps data to association key */
    Function<T, byte[] /**association key*/> keyer;

    /** multimap of association key to keys in storage */
    MutableMap<byte[] /**association key*/, MutableLongList /*storage keys*/>[] indexes;

    /** stores key epoch, shared across cache */
    TimeKeys timeKeys;

    @Getter
    final IndexMetrics metrics = new IndexMetrics();
    
    /** Max number of values stored under a key. Default is unlimited (-1). */
    int keyCapacity = -1;


    Index(String name, Function<T, byte[]> keyer, TimeKeys tk, int mapCount, int keyCapacity) {
        this.name = name;
        this.keyer = keyer;
        this.timeKeys = tk;
        this.keyCapacity = keyCapacity;

        this.indexes = new MutableMap[mapCount];

        for (int idx = 0; idx < indexes.length; idx++) {
            UnifiedMapWithHashingStrategy<byte[], MutableLongList> map = new UnifiedMapWithHashingStrategy<>(bytesHashing);
            MutableMap<byte[], MutableLongList> mmap = map.asSynchronized();
            this.indexes[idx] = mmap;
        }
    }


    /** fetch index based on key */
    MutableMap<byte[], MutableLongList> index(byte[] key) {
        int idx = Math.abs( bytesHashing.computeHashCode(key) ) % indexes.length;
        return indexes[idx];
    }


    public boolean put(T val, long storageKey) {
        metrics.putCount.incrementAndGet();
        long t = -System.nanoTime();
        try {
            byte[] key = keyer.apply(val);
            if (key == null) return false;
            MutableLongList keyData = index(key).getIfAbsentPut(key,
                    () -> LongLists.mutable
                                    .withInitialCapacity(1)
                                    .asSynchronized());
            keyData.add(storageKey);
            removedOldestKeys(keyData);
            return true;
        }
        finally {
            t += System.nanoTime();
            metrics.putDuration.addAndGet(t);
        }
    }
    
    void removedOldestKeys(MutableLongList keys) {
        if (keyCapacity <= 0) return;
        while (keys.size() > keyCapacity) {
            long min = keys.min();
            keys.remove(min);
        }
    }


    public MutableLongList get(T val) {
        metrics.getCount.incrementAndGet();
        long t = -System.nanoTime();
        try {
            byte[] key = keyer.apply(val);
            if (key == null) return null;
            MutableLongList keyData = index(key).get(key);
            return keyData;
        }
        finally {
            t += System.nanoTime();
            metrics.getDuration.addAndGet(t);
        }
    }


    /**
     * Values of T which happened on same tstamp by {@link TimeKeys#equalSec(long, long)}
     */
    public MutableLongList onSameTime(T val, long valTstamp) {
        metrics.onSameTimeCount.incrementAndGet();
        long t = -System.nanoTime();
        try {
            byte[] valKey = keyer.apply(val);
            if (valKey == null) return null;
            MutableLongList keyData = index(valKey).get(valKey);
            if (keyData == null) return null;

            boolean matching = keyData.anySatisfy(storedKey -> {
                long keyTstamp = timeKeys.tstamp(storedKey);
                boolean sameTime = timeKeys.equalSec(valTstamp, keyTstamp);
                return sameTime;
            });

            if (!matching) return null;

            MutableLongList onSameTime = keyData.select(storedKey -> {
                long keyTstamp = timeKeys.tstamp(storedKey);
                boolean sameTime = timeKeys.equalSec(valTstamp, keyTstamp);
                return sameTime;
            });

            return onSameTime;
        }
        finally {
            t += System.nanoTime();
            metrics.onSameTimeDuration.addAndGet(t);
        }
    }


    /**
     * Clear index of all associated storage keys older than limit.
     *
     * @param val - associated value
     * @param upperTstampExclusive - age limit
     */
    void clearKey(T val, long upperTstampExclusive) {
        long limit = upperTstampExclusive / 1000l * 1000l;
        metrics.clearKeyCount.incrementAndGet();
        long t = -System.nanoTime();
        try {
            byte[] key = keyer.apply(val);
            if (key == null) return;

            MutableLongList keyData = index(key).get(key);
            if (keyData == null) return;
            if (keyData.isEmpty()) return;

            MutableLongList tmpBuffer = null;

            for (int idx = 0; idx < keyData.size(); idx++) {
                long storageKey = keyData.get(idx);
                long tstamp = timeKeys.tstamp(storageKey);
                if (tstamp < limit) {
                    if (tmpBuffer == null) tmpBuffer = LongLists.mutable.empty();
                    tmpBuffer.add(storageKey);
                }
            }

            if (tmpBuffer == null) return;
            if (tmpBuffer.size() == 0) return;
            keyData.removeAll(tmpBuffer);
        }
        finally {
            t += System.nanoTime();
            metrics.clearKeyDuration.addAndGet(t);
        }
    }

    /**
     * Remove all index entries without any storage keys.
     */
    void removeEmptyEntries() {
        long dur = -System.nanoTime();
        for(MutableMap<byte[], MutableLongList> index: indexes)
            index.removeIf((key, val) -> {
                if (val == null || val.isEmpty()) {
                    metrics.removeEmptyKeyCout.incrementAndGet();
                    return true;
                }
                return false;
            });
        dur += System.nanoTime();
        metrics.removeEmptyDuration.addAndGet(dur);
    }



    class IndexMetrics implements Metrics {
        @Getter String name = "Index";
        long startTstamp = System.currentTimeMillis();

        final AtomicLong putCount = new AtomicLong();
        final AtomicLong putDuration = new AtomicLong();

        final AtomicLong getCount = new AtomicLong();
        final AtomicLong getDuration = new AtomicLong();

        final AtomicLong clearKeyCount = new AtomicLong();
        final AtomicLong clearKeyDuration = new AtomicLong();

        final AtomicLong onSameTimeCount = new AtomicLong();
        final AtomicLong onSameTimeDuration = new AtomicLong();

        final AtomicLong removeEmptyKeyCout = new AtomicLong();
        final AtomicLong removeEmptyDuration = new AtomicLong();


        @Override
        public String text(boolean comments) {
            String startStr = TimeUtils.readableTstamp(startTstamp);


            String put        = info("          put", putCount, putDuration);
            String get        = info("          get", getCount, getDuration);
            String clearKey   = info("     clearKey", clearKeyCount, clearKeyDuration);
            String onSameTime = info("   onSameTime", onSameTimeCount, onSameTimeDuration);

            long size = 0;
            for (MutableMap<byte[], MutableLongList> index: indexes) {
                size += index.size();
            }

            StringBuilder sb = new StringBuilder();
            sb.append("## ").append(name).append(" ").append(Index.this.name).append(" metrics\n");
            sb.append("  startTstamp: ").append(startStr).append("\n");
            sb.append("         size: ").append(size).append("\n");
            sb.append(put).append("\n");
            sb.append(get).append("\n");
            sb.append(clearKey).append("\n");
            sb.append(onSameTime).append("\n");
            sb.append("   empty keys: ").append(removeEmptyKeyCout.get()).append("\n");
            sb.append(" emptying dur: ").append(TimeUtils.toReadable(removeEmptyDuration.get()));
            return sb.toString();
        }

        @Override
        public String reset() {
            String ts = text(false);
            putCount.set(0);
            putDuration.set(0);
            getCount.set(0);
            getDuration.set(0);
            clearKeyCount.set(0);
            clearKeyDuration.set(0);
            onSameTimeCount.set(0);
            onSameTimeDuration.set(0);
            removeEmptyKeyCout.set(0);
            removeEmptyDuration.set(0);
            return ts;
        }
    }

}
