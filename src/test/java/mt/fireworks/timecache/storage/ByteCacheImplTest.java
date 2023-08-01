package mt.fireworks.timecache.storage;

import static junit.framework.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.Function;

import org.junit.Test;

import lombok.AllArgsConstructor;
import lombok.Data;
import mt.fireworks.timecache.SerDes2;

public class ByteCacheImplTest {

    @Data
    @AllArgsConstructor
    public static class TstTrx {
        long tstamp;
        int val;
    }

    SerDes2<TstTrx> serdes2 = new SerDes2<ByteCacheImplTest.TstTrx>() {

        public byte[] marshall(TstTrx t) {
            ByteBuffer bb = ByteBuffer.allocate(12);
            bb.putLong(t.tstamp);
            bb.putInt(t.val);
            byte[] array = bb.array();
            return array;
        }

        public TstTrx unmarshall(byte[] data) {
            ByteBuffer bb = ByteBuffer.wrap(data);
            long tstamp = bb.getLong();
            int val = bb.getInt();
            return new TstTrx(tstamp, val);
        }

        public long timestampOfT(TstTrx val) {
            return val.tstamp;
        }
    };


    Function<TstTrx, byte[]> keyer = t -> {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(t.val);
        byte[] array = bb.array();
        return array;
    };


    @Test
    public void usageExample() throws InterruptedException {
        StorageLongKey storage = StorageLongKey.init();
        Index<TstTrx> index = new Index<>(keyer, new TimeKeys());

        Index<TstTrx>[] indexes = new Index[1];
        indexes[0] = index;

        long now = System.currentTimeMillis();
        TstTrx t1 = new TstTrx(now, 1);
        TstTrx t2 = new TstTrx(now + 10, 2);
        TstTrx t3 = new TstTrx(now + 20, 1);
        TstTrx t4 = new TstTrx(now + 30, 2);
        TstTrx t5 = new TstTrx(now + 50, 1);

        ByteCacheImpl<TstTrx> cache = new ByteCacheImpl<>(storage, indexes, serdes2);
        cache.add(t1);
        cache.add(t2);
        cache.add(t3);
        cache.add(t4);
        cache.add(t5);

        Object[] res1 = cache.getArray(t1);
        assertEquals(indexes.length * 2, res1.length);
        ArrayList<TstTrx> ts1 = (ArrayList<ByteCacheImplTest.TstTrx>) res1[1];
        assertEquals(3, ts1.size());

        Object[] res2 = cache.getArray(t2);
        assertEquals(indexes.length * 2, res2.length);
        ArrayList<TstTrx> ts2 = (ArrayList<ByteCacheImplTest.TstTrx>) res2[1];
        assertEquals(2, ts2.size());
    }

}
