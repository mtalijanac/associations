package mt.fireworks.timecache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.function.Function;

import lombok.Setter;

public class BytesKeyedCacheFactory<T> {

    @Setter HashMap<String, Function<T, byte[]>> keyers = new HashMap<>();

    @Setter SerDes<T> serdes;
    @Setter Boolean metricsEnabled = Boolean.TRUE;

    @Setter Storage.Conf storageConf = new Storage.Conf();
    Long startTimestamp;
    @Setter TimeKeys timeKeys = new TimeKeys();

    @Setter boolean checkForDuplicates = false;

    public BytesKeyedCache<T> getInstance() {
        if (serdes == null)
            throw new RuntimeException("Serdes not set");

        SerDes<T> ser = serdes;
        if (metricsEnabled) {
            ser = new MetricSerDes2<>(serdes);
        }

        ArrayList<Index<T>> indexList = new ArrayList<>();
        for (Entry<String, Function<T, byte[]>> e: keyers.entrySet()) {
            String name = e.getKey();
            Function<T, byte[]> keyer = e.getValue();
            Index<T> i = new Index<T>(name, keyer, timeKeys);
            indexList.add(i);
        }

        @SuppressWarnings("unchecked")
        Index<T>[] indexes = indexList.toArray(new Index[indexList.size()]);
        Storage storage = Storage.init(storageConf, startTimestamp, timeKeys);
        BytesKeyedCache<T> cache = new BytesKeyedCache<>(storage, indexes, ser, timeKeys);
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

    public void setWinCapacity(Integer val) {
        storageConf.setWinCapacity(val);
    }

    public long setStartTimeMillis(Long startTimestamp) {
        this.startTimestamp = (startTimestamp / 1000l) * 1000l;
        return this.startTimestamp;
    }

}
