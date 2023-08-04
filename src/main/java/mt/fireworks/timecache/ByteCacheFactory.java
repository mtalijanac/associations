package mt.fireworks.timecache;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import lombok.*;

public class ByteCacheFactory<T> {

    @Setter ArrayList<Function<T, byte[]>> keyers = new ArrayList<>();

    @Setter SerDes<T> serdes;
    @Setter Boolean metricsEnabled = Boolean.TRUE;

    @Setter StorageLongKey.Conf storageConf;
    @Setter Long startTimestamp;
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
        for (Function<T, byte[]> key: keyers) {
            Index<T> i = new Index<T>(key, timeKeys);
            indexList.add(i);
        }

        Index<T>[] indexes = indexList.toArray(new Index[indexList.size()]);
        StorageLongKey storage = StorageLongKey.init(storageConf, startTimestamp, timeKeys);
        ByteCacheImpl<T> cache = new ByteCacheImpl<T>(storage, indexes, ser);
        cache.setCheckForDuplicates(checkForDuplicates);
        return cache;
    }


    public void addKeyers(Function<T, byte[]>... keys) {
        for (Function<T, byte[]> k: keys) {
            Function<T, byte[]> key = (metricsEnabled) ? new MetricKeyer<>(k, "key " + (keyers.size() + 1))
                                                       : k;
            keyers.add(key);
        }
    }

    public void storageConf(
        Integer historyWindowCount,
        Integer futureWindowCount,
        Long windowTimespanMs
    ) {
        storageConf = new StorageLongKey.Conf(historyWindowCount, futureWindowCount, windowTimespanMs);
    }

}
