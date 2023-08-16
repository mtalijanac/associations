package mt.fireworks.timecache;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Function;

import org.eclipse.collections.api.collection.primitive.MutableLongCollection;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import mt.fireworks.timecache.ByteList.ForEachAction;
import mt.fireworks.timecache.Storage.Window;

@Slf4j
@RequiredArgsConstructor
public class BytesKeyedCache<T> implements TimeCache<T, byte[]> {

    @NonNull Storage storage;
    @NonNull Index<T>[] indexes;
    @NonNull SerDes<T> serdes2;
    @NonNull TimeKeys timeKeys;

    /** enabled/disable check if data is already stored in cache */
    @Setter boolean checkForDuplicates = false;

    final BytesKeyedCacheMetrics metrics = new BytesKeyedCacheMetrics();

    public Metrics getMetrics() {
        return metrics;
    }

    @Override
    public boolean add(T val) {
        metrics.addCount.incrementAndGet();

        long tstamp = serdes2.timestampOfT(val);
        byte[] data = serdes2.marshall(val);


        if (checkForDuplicates) {
            for (Index<T> i: indexes) {
                MutableLongCollection onSameTime = i.onSameTime(val, tstamp);
                if (onSameTime == null) continue;
                boolean hasDuplicate = onSameTime.anySatisfy(copyKey -> {
                    return storage.equal(copyKey, data, serdes2);
                });
                if (hasDuplicate) {
                    metrics.foundDuplicateCount.incrementAndGet();
                    return false;
                }
            }
        }

        long storageIdx = storage.addEntry(tstamp, data);
        if (storageIdx == 0) {
            return false;
        }

        for (Index<T> i: indexes) {
            i.put(val, storageIdx);
        }

        return true;
    }


    public List<CacheEntry<byte[], List<T>>> get(T query, Long fromInclusive, Long toExclusive) {
        metrics.getCount.incrementAndGet();

        List<CacheEntry<byte[], List<T>>> resultList = new ArrayList<>(indexes.length);
        // FIXME should be lazy
        MutableLongList keysForRemoval = LongLists.mutable.empty();

        for (int idx = 0; idx < indexes.length; idx++) {
            Index<T> index = indexes[idx];
            byte[] key = index.getKeyer().apply(query);
            if (key == null) continue;

            MutableLongCollection strKeys = index.get(query);
            if (strKeys == null) continue;
            if (strKeys.isEmpty()) continue;

            String name = index.getName();
            ArrayList<T> ts = new ArrayList<>(strKeys.size());
            CacheEntry<byte[], List<T>> entry = new CacheEntry<>(name, key, ts);
            resultList.add(entry);

            strKeys.forEach(strKey -> {
                long tstamp = timeKeys.tstamp(strKey);
                if (fromInclusive != null) {
                    long from = fromInclusive.longValue() / 1000l * 1000l;
                    if (tstamp < from) return;
                }
                if (toExclusive != null) {
                    long to = toExclusive.longValue() / 1000l * 1000l + 1000l;
                    if (tstamp > to) return;
                }

                T res = storage.getEntry2(strKey, serdes2);
                if (res == null) {
                    keysForRemoval.add(strKey);
                    return;
                }

                if (fromInclusive != null || toExclusive != null) {
                    long timestamp = serdes2.timestampOfT(res);
                    if (fromInclusive != null && timestamp < fromInclusive) return;
                    if (toExclusive != null && timestamp >= toExclusive) return;
                }

                ts.add(res);
            });

            metrics.trxGetCount.addAndGet(ts.size());

            if (keysForRemoval != null && keysForRemoval.size() > 0) {
                strKeys.removeAll(keysForRemoval);
                keysForRemoval.clear();
            }
        }

        return resultList;
    }



    final ReentrantReadWriteLock tickLock = new ReentrantReadWriteLock();


    @Override
    public void tick() {
        metrics.tickCount.incrementAndGet();
        metrics.lastTickStart.set( System.currentTimeMillis() );
        long start = System.nanoTime();
        metrics.lastWindowSize.set(0);

        // add new and, remove obsolete window from storage
        WriteLock wock = tickLock.writeLock();
        wock.lock();
        Window removedWindow = storage.moveWindows();
        wock.unlock();

        // clean indexes
        long endTstamp = removedWindow.endTstamp;
        removedWindow.store.forEach((objPos, bucket, pos, len) -> {
            metrics.lastWindowSize.incrementAndGet();
            T obj = serdes2.unmarshall(bucket, pos, len);
            for(Index<T> idx: indexes) {
                idx.clearKey(obj, endTstamp);
            }
            return ForEachAction.CONTINUE;
        });

        for(Index<T> idx: indexes) {
            idx.removeEmptyEntries();
        }

        long count = metrics.lastWindowSize.get();
        metrics.objectsRemovedTotal.addAndGet(count);
        long end = System.nanoTime();
        long duration = end - start;
        metrics.lastTickDuration.set( duration );
    }


    @Data
    static class BytesKeyedCacheMetrics implements Metrics {
        String name = "BytesKeyedCache";

        long startTstamp = System.currentTimeMillis();

        final AtomicLong addCount = new AtomicLong();
        final AtomicLong foundDuplicateCount = new AtomicLong();

        final AtomicLong getCount = new AtomicLong();
        final AtomicLong trxGetCount = new AtomicLong();

        final AtomicLong tickCount = new AtomicLong();
        final AtomicLong objectsRemovedTotal = new AtomicLong();
        final AtomicLong lastWindowSize = new AtomicLong();
        final AtomicLong lastTickStart = new AtomicLong();
        final AtomicLong lastTickDuration = new AtomicLong();



        @Override
        public String text(boolean comments) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            Date start = new Date(startTstamp);
            String startStr = sdf.format(start);
            String tickStart = sdf.format(new Date(lastTickStart.get()));
            long duration = lastTickDuration.get();
            String durStr = TimeUtils.toReadable(duration);

            StringBuilder sb = new StringBuilder();
            sb.append("## ").append(name).append(" metrics\n");
            sb.append("  startTstamp: ").append(startStr).append(" (metrics creation date) \n");
            sb.append("     addCount: ").append(addCount.get())
              .append(" (including ").append(foundDuplicateCount.get()).append(" duplicates)")
              .append(comments ? "    // number of cache writes\n" : "\n");

            sb.append("     getCount: ").append(getCount.get())
               .append(comments ? "    // number of cache reads\n" : "\n");

            sb.append("  trxGetCount: ").append(trxGetCount.get())
              .append(comments ? "    // total count of read transactions\n" : "\n");

            sb.append("    tickCount: ").append(tickCount.get()).append("\n");
            sb.append("      cleaned: ").append(objectsRemovedTotal.get()).append(" objects total\n");

            sb.append("  last window: ").append(lastWindowSize.get()).append(" objects")
              .append(comments ? "    // number of objects in last cleaned window\n" : "\n");

            sb.append("   last tickt: ").append(tickStart)
              .append(comments ? "    // timestamp of last tick\n" : "\n");

            sb.append("tick duration: ").append(durStr)
              .append(comments ? "    // last tick duration " : "");

            return sb.toString();
        }

        @Override
        public String reset() {
            String text = text(false);
            startTstamp = System.currentTimeMillis();

            tickCount.set(0);
            objectsRemovedTotal.set(0);
            lastWindowSize.set(0);
            lastTickStart.set(0);
            lastTickDuration.set(0);

            getCount.set(0);
            trxGetCount.set(0);
            addCount.set(0);
            foundDuplicateCount.set(0);

            return text;
        }
    }


    public String allMetrics() {
        ArrayList<Metrics> ms = new ArrayList<>();
        ms.add(metrics);
        if (serdes2 instanceof Metrics) {
            ms.add((Metrics) serdes2);
        }
        for (Index<?> i: indexes) {
            ms.add(i.getMetrics());
            Function<?, byte[]> keyer = i.getKeyer();
            if (keyer instanceof Metrics) {
                ms.add((Metrics) keyer);
            }
        }
        ms.add(storage.getMetric());

        StringBuilder sb = new StringBuilder();
        for (Metrics m: ms) {
            sb.append(m.text(true));
            sb.append("\n\n");
        }

        return sb.toString();
    }


    public long startTimeMillis() {
        return storage.nowWindow.startTstamp;
    }

}
