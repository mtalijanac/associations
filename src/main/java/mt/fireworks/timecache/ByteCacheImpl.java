package mt.fireworks.timecache;

import java.util.*;
import java.util.Map.Entry;
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
public class ByteCacheImpl<T> implements TimeCache<T, byte[]>{

    @NonNull StorageLongKey storage;
    @NonNull Index<T>[] indexes;
    @NonNull SerDes<T> serdes2;

    /** enabled/disable check if data is already stored in cache */
    @Setter boolean checkForDuplicates = false;

    @Override
    public boolean add(T val) {
        long tstamp = serdes2.timestampOfT(val);
        byte[] data = serdes2.marshall(val);


        if (checkForDuplicates) {
            for (Index<T> i: indexes) {
                MutableLongCollection onSameTime = i.onSameTime(val, tstamp);
                if (onSameTime == null) continue;
                boolean hasDuplicate = onSameTime.anySatisfy(copyKey -> {
                    return storage.equal(copyKey, data, serdes2);
                });
                if (hasDuplicate) return false;
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
    public List<Entry<byte[], List<T>>> get(T val) {
        List<Entry<byte[], List<T>>> resultList = new ArrayList<>(indexes.length);
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
            CacheEntry<T> entry = new CacheEntry<>(name, key, ts);
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

        long t1 = System.nanoTime();
        // add new and, remove obsolete window from storage
        WriteLock wock = tickLock.writeLock();
        wock.lock();
        Window removedWindow = storage.moveWindows();
        wock.unlock();

        long t2 = System.nanoTime();
        long mwDur = t2 - t1;
        AtomicLong objCounter = new AtomicLong();

        // clean indexes
        long endTstamp = removedWindow.endTstamp;
        removedWindow.store.forEach((objPos, bucket, pos, len) -> {
            objCounter.incrementAndGet();
            T obj = serdes2.unmarshall(bucket, pos, len);
            for(Index<T> idx: indexes) {
                idx.clearKey(obj, endTstamp);
            }
            return ForEachAction.CONTINUE;
        });

        long t3 = System.nanoTime();
        long ic = t3 - t2;
        long objCount = objCounter.get();

        // FIXME add metrics and logging

        if (log.isDebugEnabled()) {
            String durReadable = TimeUtils.toReadable(mwDur);
            String icReadable = TimeUtils.toReadable(ic);
            log.debug("New window: '{}', index cleaned: '{}', obj count: '{}'", durReadable, icReadable, objCount);
        }
    }

    @Override
    public String toString() {
        String res = "";
        if (serdes2 instanceof MetricSerDes2) {
            String serdesMetric = ((MetricSerDes2<T>) serdes2).resetMetrics();
            res += serdesMetric;
        }
        for (Index<T> idx: indexes) {
            Function<T, byte[]> keyer = idx.getKeyer();
            if (keyer instanceof MetricKeyer) {
                String keyerMetrics = ((MetricKeyer<T, byte[]>) keyer).resetMetrics();
                res += "\n" + keyerMetrics;
            }
        }
        return res;
    }

}
