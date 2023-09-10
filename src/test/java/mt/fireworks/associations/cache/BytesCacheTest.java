package mt.fireworks.associations.cache;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import lombok.AllArgsConstructor;
import lombok.Data;

public class BytesCacheTest {

    @Data
    @AllArgsConstructor
    public static class TstTrx {
        long tstamp;
        int val;
    }

    CacheSerDes<TstTrx> serdes2 = new CacheSerDes<BytesCacheTest.TstTrx>() {

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
        BytesCacheFactory<TstTrx> factory = new BytesCacheFactory<>();
        factory.setSerdes(serdes2);
        factory.addKeyer("key", keyer);
        BytesCache<TstTrx> cache = factory.getInstance();

        long now = System.currentTimeMillis();
        TstTrx t1 = new TstTrx(now, 1);
        TstTrx t2 = new TstTrx(now + 10, 2);
        TstTrx t3 = new TstTrx(now + 20, 1);
        TstTrx t4 = new TstTrx(now + 30, 2);
        TstTrx t5 = new TstTrx(now + 50, 1);

        cache.add(t1);
        cache.add(t2);
        cache.add(t3);
        cache.add(t4);
        cache.add(t5);

        Map<String, List<TstTrx>> res1 = cache.getAsMap(t1);
        assertEquals(1, res1.size());
        List<TstTrx> ts1 = res1.get("key");
        assertEquals(3, ts1.size());

        Map<String, List<TstTrx>> res2 = cache.getAsMap(t2);
        assertEquals(1, res2.size());
        List<TstTrx> ts2 = res2.get("key");
        assertEquals(2, ts2.size());
    }

    @Test
    public void duplicateTest() {
        BytesCacheFactory<TstTrx> factory = new BytesCacheFactory<>();
        factory.setSerdes(serdes2);
        factory.addKeyer("Example", keyer);

        BytesCache<TstTrx> cache = factory.getInstance();
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

    @Test
    public void timeLogicTEst() {
        BytesCache<TstTrx> cache = BytesCache.newInstance(TstTrx.class)
             .withSerdes(serdes2)
             .associate("key", keyer)
             .build();

        long t = System.currentTimeMillis();
        cache.add( new TstTrx(t, 1) );
        cache.add( new TstTrx(t + 1000, 1) );
        cache.add( new TstTrx(t + 2000, 1) );
        cache.add( new TstTrx(t + 3000, 1) );
        cache.add( new TstTrx(t + 4000, 1) );

        TstTrx q0 = new TstTrx(t + 10_000, 1);
        Map<String, List<TstTrx>> m0 = cache.getAsMap(q0);
        assertEquals(5, m0.get("key").size());

        // test before

        List<TstTrx> res_1 = cache.getAsMap(q0, null, null).get("key");
        assertEquals(5, res_1.size());

        List<TstTrx> res_2 = cache.getAsMap(q0, null, t).get("key");
        assertEquals(0, res_2.size());

        List<TstTrx> res_3 = cache.getAsMap(q0, null, t - 1).get("key");
        assertEquals(0, res_3.size());

        List<TstTrx> res_4 = cache.getAsMap(q0, null, t + 11).get("key");
        assertEquals(1, res_4.size());

        List<TstTrx> res_5 = cache.getAsMap(q0, null,   t + 1000l).get("key");
        assertEquals(1, res_5.size());

        List<TstTrx> res_6 = cache.getAsMap(q0, null, t + 1001l).get("key");
        assertEquals(2, res_6.size());

        List<TstTrx> res_7 = cache.getAsMap(q0, t + 1999, null).get("key");
        assertEquals(3, res_7.size());

        List<TstTrx> res_8 = cache.getAsMap(q0, t + 2000, null).get("key");
        assertEquals(3, res_8.size());

        List<TstTrx> res_9 = cache.getAsMap(q0, t + 2001, null).get("key");
        assertEquals(2, res_9.size());

        List<TstTrx> res_10 = cache.getAsMap(q0, t - 1000, t + 10000).get("key");
        assertEquals(5, res_10.size());

        List<TstTrx> res_11 = cache.getAsMap(q0, t, t + 4001).get("key");
        assertEquals(5, res_11.size());

        List<TstTrx> res_12 = cache.getAsMap(q0, t, t + 4000).get("key");
        assertEquals(4, res_12.size());

        List<TstTrx> res_13 = cache.getAsMap(q0, t + 1, t + 4000).get("key");
        assertEquals(3, res_13.size());
    }
}

