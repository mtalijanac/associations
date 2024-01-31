package mt.fireworks.associations.cache;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.eclipse.collections.api.collection.primitive.MutableLongCollection;
import org.eclipse.collections.api.list.primitive.*;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import lombok.*;
import mt.fireworks.associations.AssociationCache;
import mt.fireworks.associations.cache.BytesCacheBuilder.AddSerdes;
import mt.fireworks.associations.cache.Storage.Window;

@RequiredArgsConstructor
public class BytesCache<T> implements AssociationCache<T> {

    @NonNull TimeKeys timeKeys;         // stores key epoch
    @NonNull Storage storage;           // stores data
    @NonNull Index<T>[] indexes;        // stores associations
    @NonNull CacheSerDes<T> serdes2;    // does data manipulation

    List<String> keys;


    /** enabled/disable check if data is already stored in cache */
    @Setter boolean checkForDuplicates = false;

    @Getter
    final BytesKeyedCacheMetrics metrics = new BytesKeyedCacheMetrics();

    public static <T> AddSerdes<T> newInstance(Class<T> klazz) {
        return new BytesCacheBuilder.Builder<T>();
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
    public List<T> get(String keyName, T query) {
        return get(keyName, query, null,  null);
    }


    @Override
    public List<T> get(String indexName, T query, Long fromInclusive, Long toExclusive) {
        return getLast(indexName, query, -1, fromInclusive, toExclusive);
    }


    @Override
    public List<T> getLast(String indexName, T query, Integer count, Long fromInclusive, Long toExclusive) {
        metrics.getCount.incrementAndGet();

        Index<T> index = null;
        for (int i = 0; i < indexes.length; i++) {
            index = indexes[i];
            if (indexName.equals(index.getName())) {
                break;
            }
        }

        if (index == null) return Collections.emptyList();

        List<T> result = readIndex(index, query, count, fromInclusive, toExclusive);
        metrics.trxGetCount.addAndGet(result.size());
        return result;
    }


    @Override
    public Map<String, List<T>> getAsMap(T query) {
        return getAsMap(query, null, null);
    }


    @Override
    public Map<String, List<T>> getAsMap(T query, Long fromInclusive, Long toExclusive) {
        metrics.getCount.incrementAndGet();

        int capacity = Math.round(indexes.length / 0.75f) + 1;
        UnifiedMap<String, List<T>> result = new UnifiedMap<>(capacity);

        for (int i = 0; i < indexes.length; i++) {
            Index<T> index = indexes[i];
            List<T> res = readIndex(index, query, fromInclusive, toExclusive);
            if (res == null) continue;
            result.put(index.getName(), res);
            metrics.trxGetCount.addAndGet(res.size());
        }

        return result;
    }


    List<T> readIndex(Index<T> index, T query, Long fromInclusive, Long toExclusive) {
        return readIndex(index, query, -1, fromInclusive, toExclusive);
    }

    List<T> readIndex(Index<T> index, T query, int countLast, Long fromInclusive, Long toExclusive) {
        byte[] key = index.getKeyer().apply(query);
        if (key == null) return null;


        //
        // FIXME index should already have immutable long list
        //       which is updated on put. Move cost of thread
        //       safety to put side of index
        //
        MutableLongList storageKeysMut = index.get(query);
        if (storageKeysMut == null)   return Collections.emptyList();
        if (storageKeysMut.isEmpty()) return Collections.emptyList();
        LongList strKeys = storageKeysMut.toImmutable();

        int size = strKeys.size();
        int startIdx = countLast > size ? size - countLast : 0;

        MutableLongList keysForRemoval = null;
        ArrayList<T> result = new ArrayList<>(countLast > 0 ? countLast : size);

        for (int jdx = startIdx; jdx < size; jdx++) {
            long strKey = strKeys.get(jdx);
            long tstamp = timeKeys.tstamp(strKey);

            if (fromInclusive != null) {
                long from = fromInclusive.longValue() / 1000l * 1000l;
                if (tstamp < from) continue;
            }
            if (toExclusive != null) {
                long to = toExclusive.longValue()     / 1000l * 1000l + 1000l;
                if (tstamp > to) continue;
            }

            T res = storage.getEntry2(strKey, serdes2);
            if (res == null) {
                if (keysForRemoval == null) {
                    keysForRemoval = LongLists.mutable.empty();
                }
                keysForRemoval.add(strKey);
                continue;
            }

            if (fromInclusive != null || toExclusive != null) {
                long timestamp = serdes2.timestampOfT(res);
                if (fromInclusive != null && timestamp < fromInclusive) continue;
                if (toExclusive != null && timestamp >= toExclusive) continue;
            }

            result.add(res);
        }

        if (keysForRemoval != null && keysForRemoval.size() > 0) {
            storageKeysMut.removeAll(keysForRemoval);
            keysForRemoval.clear();
        }

        return result;
    }



    @Override
    public void tick() {
        metrics.tickCount.incrementAndGet();
        metrics.lastTickStart.set( System.currentTimeMillis() );
        long start = System.nanoTime();
        metrics.lastWindowSize.set(0);

        // add new and, remove obsolete window from storage
        Window removedWindow = storage.moveWindows();

        // clean indexes
        long endTstamp = removedWindow.endTstamp;
        removedWindow.store.forEach((objPos, bucket, pos, len) -> {
            metrics.lastWindowSize.incrementAndGet();
            T obj = serdes2.unmarshall(bucket, pos, len);
            for(Index<T> idx: indexes) {
                idx.clearKey(obj, endTstamp);
            }
            return null;
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


    @Override
    public long startTimeMillis() {
        return storage.nowWindow.startTstamp;
    }


    @Override
    public List<String> keys() {
        if (keys != null)
            return keys;

        ArrayList<String> list = new ArrayList<>();
        for (Index<T> idx: indexes) {
            list.add(idx.getName());
        }
        keys = Collections.unmodifiableList(list);
        return keys;
    }


    @Override
    public Iterator<T> values() {
        Iterator<T> valueIter = storage.iterator((objPos, bucket, pos, len) -> serdes2.unmarshall(bucket, pos, len));
        return valueIter;
    }


    @Override
    public Iterator<T> indexValues(String keyName) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public Iterator<List<T>> indexAssociations(String keyName) {
        // TODO Auto-generated method stub
        return null;
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
            String startStr = TimeUtils.readableTstamp(startTstamp);
            String tickStart = TimeUtils.readableTstamp(lastTickStart.get());
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
}
