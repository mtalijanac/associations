package mt.fireworks.timecache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.function.Function;

import lombok.Setter;

public class ByteCacheFactory<T> {

    @Setter HashMap<String, Function<T, byte[]>> keyers = new HashMap<>();

    @Setter SerDes<T> serdes;
    @Setter Boolean metricsEnabled = Boolean.TRUE;

    @Setter StorageLongKey.Conf storageConf;
    Long startTimestamp;
    @Setter TimeKeys timeKeys = new TimeKeys();

    @Setter boolean checkForDuplicates = false;

    public ByteCacheImpl<T> getInstance() {
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

        Index<T>[] indexes = indexList.toArray(new Index[indexList.size()]);
        StorageLongKey storage = StorageLongKey.init(storageConf, startTimestamp, timeKeys);
        ByteCacheImpl<T> cache = new ByteCacheImpl<T>(storage, indexes, ser);
        cache.setCheckForDuplicates(checkForDuplicates);
        return cache;
    }

    public void addKeyer(String name, Function<T, byte[]> keyer) {
        keyers.put(name, keyer);
    }

    public void storageConf(
        Integer historyWindowCount,
        Integer futureWindowCount,
        Long windowTimespanMs
    ) {
        storageConf = new StorageLongKey.Conf(historyWindowCount, futureWindowCount, windowTimespanMs);
    }

    public Long setStartTimestamp(Long startTimestamp) {
        this.startTimestamp = (startTimestamp / 1000l) * 1000l;
        return this.startTimestamp;
    }

}
