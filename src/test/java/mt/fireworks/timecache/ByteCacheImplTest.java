package mt.fireworks.timecache;

import static junit.framework.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.junit.Test;

import lombok.AllArgsConstructor;
import lombok.Data;
import mt.fireworks.timecache.index.Index;
import mt.fireworks.timecache.storage.StorageLongKey;

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

        public TstTrx unmarshall(byte[] data, int position, int length) {
            ByteBuffer bb = ByteBuffer.wrap(data, position, length);
            long tstamp = bb.getLong();
            int val = bb.getInt();
            return new TstTrx(tstamp, val);
        }

        public long timestampOfT(TstTrx val) {
            return val.tstamp;
        }

        public long timestampOfD(byte[] data, int position, int length) {
            long tstamp = ByteBuffer.wrap(data, position, length).getLong();
            return tstamp;
        }

        public boolean equalsT(TstTrx t, TstTrx u) {
            return t.equals(u);
        }

        public boolean equalsD(byte[] data1, byte[] data2) {
            return Arrays.equals(data1, data2);
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
        Index<TstTrx> index = new Index<>();
        index.setKeyer(keyer);

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
        ArrayList<TstTrx> ts1 = (ArrayList<ByteCacheImplTest.TstTrx>) res1[0];
        assertEquals(3, ts1.size());

        Object[] res2 = cache.getArray(t2);
        ArrayList<TstTrx> ts2 = (ArrayList<ByteCacheImplTest.TstTrx>) res2[0];
        assertEquals(2, ts2.size());
    }

}
