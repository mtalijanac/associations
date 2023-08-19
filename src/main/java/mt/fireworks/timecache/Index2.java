package mt.fireworks.timecache;

import static mt.fireworks.timecache.TimeUtils.info;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.eclipse.collections.api.block.HashingStrategy;
import org.eclipse.collections.api.collection.primitive.MutableLongCollection;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.map.strategy.mutable.UnifiedMapWithHashingStrategy;

import lombok.Data;
import lombok.Getter;

@Data
class Index2<T> {

    String name;
    MutableMap<byte[], MutableLongCollection> index;
    Function<T, byte[]> keyer;
    TimeKeys timeKeys;

    final IndexMetrics metrics = new IndexMetrics();

    public Metrics getMetrics() {
        return metrics;
    }


    Index2(String name, Function<T, byte[]> keyer, TimeKeys tk) {
        this.name = name;
        this.keyer = keyer;
        this.timeKeys = tk;

        UnifiedMapWithHashingStrategy<byte[], MutableLongCollection> map = new UnifiedMapWithHashingStrategy<>(new IndexHashCode());
        index = map.asSynchronized();
    }


    public boolean put(T val, long storageKey) {
        metrics.putCount.incrementAndGet();
        long t = -System.nanoTime();
        try {
            byte[] key = keyer.apply(val);
            if (key == null) return false;
            MutableLongCollection keyData = index.getIfAbsentPut(key, () -> LongLists.mutable.empty().asSynchronized());
            keyData.add(storageKey);
            return true;
        }
        finally {
            t += System.nanoTime();
            metrics.putDuration.addAndGet(t);
        }
    }

    public MutableLongCollection get(T val) {
        metrics.getCount.incrementAndGet();
        long t = -System.nanoTime();
        try {
            byte[] key = keyer.apply(val);
            if (key == null) return null;
            MutableLongCollection keyData = index.get(key);
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
    public MutableLongCollection onSameTime(T val, long valTstamp) {
        metrics.onSameTimeCount.incrementAndGet();
        long t = -System.nanoTime();
        try {
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
        finally {
            t += System.nanoTime();
            metrics.onSameTimeDuration.addAndGet(t);
        }
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
        long limit = upperTstampExclusive / 1000l * 1000l;
        metrics.clearKeyCount.incrementAndGet();
        long t = -System.nanoTime();
        try {
            byte[] key = keyer.apply(val);
            if (key == null) return;

            MutableLongCollection keyData = index.get(key);
            if (keyData == null) return;
            if (keyData.isEmpty()) return;

            MutableLongList tmpBuffer = LongLists.mutable.empty();

            keyData.forEach(storageKey -> {
                long tstamp = timeKeys.tstamp(storageKey);
                if (tstamp < limit) {
                    tmpBuffer.add(storageKey);
                }
            });

            if (tmpBuffer.size() == 0) return;
            keyData.removeAll(tmpBuffer);
        }
        finally {
            t += System.nanoTime();
            metrics.clearKeyDuration.addAndGet(t);
        }
    }

    public void removeEmptyEntries() {
        long dur = -System.nanoTime();
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

            StringBuilder sb = new StringBuilder();
            sb.append("## ").append(name).append(" ").append(Index2.this.name).append(" metrics\n");
            sb.append("  startTstamp: ").append(startStr).append("\n");
            sb.append("         size: ").append(index.size()).append("\n");
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
