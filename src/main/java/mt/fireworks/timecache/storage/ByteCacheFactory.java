package mt.fireworks.timecache.storage;

import java.util.ArrayList;
import java.util.function.Function;

import lombok.Setter;
import mt.fireworks.timecache.SerDes2;
import mt.fireworks.timecache.index.Index;

public class ByteCacheFactory<T> {

    @Setter ArrayList<Function<T, byte[]>> keyers = new ArrayList<>();

    @Setter SerDes2<T> serdes;
    @Setter Boolean metricsEnabled = Boolean.TRUE;

    @Setter StorageLongKey.Conf storageConf;
    @Setter Long startTimestamp;

    public ByteCacheImpl<T> getInstance() {
        if (serdes == null)
            throw new NullPointerException("Serdes not set");

        SerDes2<T> ser = serdes;
        if (metricsEnabled) {
            ser = new MetricSerDes2<>(serdes);
        }

        ArrayList<Index<T>> indexList = new ArrayList<>();
        for (Function<T, byte[]> key: keyers) {
            Index<T> i = new Index<>();
            i.setKeyer(key);
            indexList.add(i);
        }

        Index[] indexes = indexList.toArray(new Index[indexList.size()]);
        StorageLongKey storage = StorageLongKey.init(storageConf, startTimestamp);
        ByteCacheImpl<T> cache = new ByteCacheImpl<>(storage, indexes, ser);
        return cache;
    }


    public void addKeyers(Function<T, byte[]>... keyer) {
        for (Function<T, byte[]> k: keyer) {
            keyers.add(k);
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
