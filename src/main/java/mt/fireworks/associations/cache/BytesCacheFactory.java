package mt.fireworks.associations.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.function.Function;

import lombok.Setter;

public class BytesCacheFactory<T> {

    @Setter HashMap<String, Function<T, byte[]>> keyers = new HashMap<>();

    @Setter CacheSerDes<T> serdes;
    @Setter Boolean metricsEnabled = Boolean.TRUE;

    @Setter Storage.Conf storageConf = new Storage.Conf();
    
    /** Max number of values stored under a key. Default is unlimited (-1). */
    @Setter int keyCapacity = -1;

    @Setter boolean checkForDuplicates = false;
    @Setter int indexMapCount = 128;
    Long startTimestamp;

    public BytesCache<T> getInstance() {
        if (serdes == null)
            throw new RuntimeException("Serdes not set");

        CacheSerDes<T> ser = serdes;
        if (metricsEnabled) {
            ser = serdes.withMetric();
        }

        TimeKeys timeKeys = new TimeKeys();
        
        ArrayList<Index<T>> indexList = new ArrayList<>();
        for (Entry<String, Function<T, byte[]>> e: keyers.entrySet()) {
            String name = e.getKey();
            Function<T, byte[]> keyer = e.getValue();
            Index<T> i = new Index<>(name, keyer, timeKeys, indexMapCount, keyCapacity);
            indexList.add(i);
        }

        @SuppressWarnings("unchecked")
        Index<T>[] indexes = indexList.toArray(new Index[indexList.size()]);
        Storage storage = new Storage(storageConf, startTimestamp, timeKeys);
        BytesCache<T> cache = new BytesCache<>(timeKeys, storage, indexes, ser);
        cache.setCheckForDuplicates(checkForDuplicates);
        return cache;
    }

    public void addKeyer(String name, Function<T, byte[]> keyer) {
        if (metricsEnabled) {
            MetricKeyer<T, byte[]> mk = new MetricKeyer<>(keyer, name);
            keyers.put(name, mk);
            return;
        }
        keyers.put(name, keyer);
    }

    public void setHistoryWindowsCount(Integer val) {
        storageConf.setHistoryWindowCount(val);
    }

    public void setFutureWindowCount(Integer val) {
        storageConf.setFutureWindowCount(val);
    }

    public void setWindowTimespanMs(Long val) {
        storageConf.setWindowTimespanMs(val);
    }

    public void setAllocationSize(int sizeInBytes) {
        storageConf.setAllocationSize(sizeInBytes);
    }
    
    public long setStartTimeMillis(Long startTimestamp) {
        this.startTimestamp = TimeKeys.normalizieTimestamp(startTimestamp);
        return this.startTimestamp;
    }
    
}
