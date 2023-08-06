package mt.fireworks.timecache;

import static junit.framework.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import lombok.AllArgsConstructor;
import lombok.Data;
import mt.fireworks.timecache.*;

public class BytesKeyedCacheTest {

    @Data
    @AllArgsConstructor
    public static class TstTrx {
        long tstamp;
        int val;
    }

    SerDes<TstTrx> serdes2 = new SerDes<BytesKeyedCacheTest.TstTrx>() {

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
        Index<TstTrx> index = new Index<>("Example", keyer, new TimeKeys());

        Index<TstTrx>[] indexes = new Index[1];
        indexes[0] = index;

        long now = System.currentTimeMillis();
        TstTrx t1 = new TstTrx(now, 1);
        TstTrx t2 = new TstTrx(now + 10, 2);
        TstTrx t3 = new TstTrx(now + 20, 1);
        TstTrx t4 = new TstTrx(now + 30, 2);
        TstTrx t5 = new TstTrx(now + 50, 1);

        BytesKeyedCache<TstTrx> cache = new BytesKeyedCache<>(storage, indexes, serdes2);
        cache.add(t1);
        cache.add(t2);
        cache.add(t3);
        cache.add(t4);
        cache.add(t5);

        List<Entry<byte[], List<TstTrx>>> res1 = cache.get(t1);
        assertEquals(1, res1.size());
        List<TstTrx> ts1 = res1.get(0).getValue();
        assertEquals(3, ts1.size());


        List<Entry<byte[], List<TstTrx>>> res2 = cache.get(t2);
        assertEquals(1, res2.size());
        List<TstTrx> ts2 = res2.get(0).getValue();
        assertEquals(2, ts2.size());
    }

    @Test
    public void duplicateTest() {
        BytesKeyedCacheFactory<TstTrx> factory = new BytesKeyedCacheFactory<>();
        factory.setSerdes(serdes2);
        factory.addKeyer("Example", keyer);

        BytesKeyedCache<TstTrx> cache = factory.getInstance();
        cache.setCheckForDuplicates(true);

        long now = System.currentTimeMillis();
        TstTrx t1 = new TstTrx(now, 1);
        TstTrx t2 = new TstTrx(now + 10, 2);

        boolean r1 = cache.add(t1);
        Assert.assertTrue(r1);

        boolean r2 = cache.add(t2);
        Assert.assertTrue(r2);

        // adding duplicate should fail
        boolean r12 = cache.add(t1);
        Assert.assertFalse(r12);
    }
}

