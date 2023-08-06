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
import mt.fireworks.timecache.StorageLongKey.Window;

@Slf4j
@RequiredArgsConstructor
public class BytesKeyedCache<T> implements TimeCache<T, byte[]> {

    @NonNull StorageLongKey storage;
    @NonNull Index<T>[] indexes;
    @NonNull SerDes<T> serdes2;

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


    @Override
    public List<CacheEntry<byte[], List<T>>> get(T val) {
        metrics.getCount.incrementAndGet();

        List<CacheEntry<byte[], List<T>>> resultList = new ArrayList<>(indexes.length);
        MutableLongList keysForRemoval = LongLists.mutable.empty();

        for (int idx = 0; idx < indexes.length; idx++) {
            Index<T> index = indexes[idx];
            byte[] key = index.getKeyer().apply(val);
            if (key == null) continue;

            MutableLongCollection strKeys = index.get(val);
            if (strKeys == null) continue;
            if (strKeys.isEmpty()) continue;

            String name = index.getName();
            ArrayList<T> ts = new ArrayList<>(strKeys.size());
            CacheEntry<byte[], List<T>> entry = new CacheEntry<byte[], List<T>>(name, key, ts);
            resultList.add(entry);

            strKeys.forEach(strKey -> {
                T res = storage.getEntry2(strKey, serdes2);
                if (res != null) ts.add(res);
            });

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
        metrics.lastTickStart.set( System.nanoTime() );
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

        long count = metrics.lastWindowSize.get();
        metrics.objectsRemovedTotal.addAndGet(count);
        metrics.lastTickEnd.set( System.nanoTime() );
    }


    @Data
    static class BytesKeyedCacheMetrics implements Metrics {
        String name = "BytesKeyedCache";

        long startTstamp = System.currentTimeMillis();

        final AtomicLong addCount = new AtomicLong();
        final AtomicLong foundDuplicateCount = new AtomicLong();

        final AtomicLong getCount = new AtomicLong();

        final AtomicLong tickCount = new AtomicLong();
        final AtomicLong objectsRemovedTotal = new AtomicLong();
        final AtomicLong lastWindowSize = new AtomicLong();
        final AtomicLong lastTickStart = new AtomicLong();
        final AtomicLong lastTickEnd = new AtomicLong();



        @Override
        public String text() {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            Date start = new Date(startTstamp);
            String startStr = sdf.format(start);
            String tickStart = sdf.format(new Date(lastTickStart.get() / 1000_000l));
            String tickEnd = sdf.format(new Date(lastTickEnd.get()));
            long duration = lastTickEnd.get() - lastTickStart.get();
            String durStr = TimeUtils.toReadable(duration);

            StringBuilder sb = new StringBuilder();
            sb.append("## ").append(name).append(" metrics\n");
            sb.append("  startTstamp: ").append(startStr).append("\n");
            sb.append("     addCount: ").append(addCount.get())
              .append(" (including ").append(foundDuplicateCount.get()).append(" duplicates)\n");
            sb.append("     getCount: ").append(getCount.get()).append("\n");
            sb.append("    tickCount: ").append(tickCount.get()).append("\n");
            sb.append("      cleaned: ").append(objectsRemovedTotal.get()).append(" objects total\n");
            sb.append("  last window: ").append(lastWindowSize.get()).append(" objects \n");
            sb.append("          started at: ").append(tickStart).append("\n");
            sb.append("            duration: ").append(durStr);

            return sb.toString();
        }

        @Override
        public String reset() {
            String text = text();
            startTstamp = System.currentTimeMillis();

            tickCount.set(0);
            objectsRemovedTotal.set(0);
            lastWindowSize.set(0);
            lastTickStart.set(0);
            lastTickEnd.set(0);

            getCount.set(0);
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

        StringBuilder sb = new StringBuilder();
        for (Metrics m: ms) {
            sb.append(m.reset());
            sb.append("\n\n");
        }

        return sb.toString();
    }

}
