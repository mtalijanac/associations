package mt.fireworks.timecache.storage;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import org.junit.Test;

import lombok.AllArgsConstructor;
import lombok.Data;
import mt.fireworks.timecache.SerDes2;
import mt.fireworks.timecache.storage.ByteCacheFactory;
import mt.fireworks.timecache.storage.ByteCacheImpl;

public class BigTest {

    @Data @AllArgsConstructor
    static class MockObj {
        final static int objSize = 2 * 8;

        long tstamp;
        long idx1;
    }


    /**
     * Store large amount of data in this single thread test.
     * Window size: 10 min day
     * expected window store count: 1 000 000 objects
     * Cache size: 7 history windows, 1 future window
     * Test run time: 30 windows
     * Index count: 1
     */
    @Test
    public void runLargeTest() {
        // indexes which be used during all testing

        long[] indexes = testIndexes();

        ByteCacheFactory<MockObj> fact = new ByteCacheFactory<>();
        fact.setSerdes(serdes);
        fact.addKeyers(keyer);

        long start = System.currentTimeMillis();
        fact.setStartTimestamp(start);

        long tenMinutes = 10 * 60 * 1000;
        fact.storageConf(7, 1, tenMinutes);

        ByteCacheImpl<MockObj> cache = fact.getInstance();

        for (int d = 0; d < 100; d++) {
            // one day
            for (int c = 0; c < 1_000_000; c++) {
                long tstamp = ThreadLocalRandom.current().nextLong(start, start + tenMinutes);

                int i = ThreadLocalRandom.current().nextInt(indexes.length);
                long index = indexes[i];

                MockObj obj = new MockObj(tstamp, index);
                cache.add(obj);
            }
            cache.tick();
            if (d == 19) {
                System.out.println(cache.toString());
            }
        }
        System.out.println(cache.toString());
    }

    long[] testIndexes() {
        int indexSize = 100_000;
        long[] indexes = new long[indexSize];
        for (int i = 0; i < indexes.length; i++)
            indexes[i] = ThreadLocalRandom.current().nextLong();
        return indexes;
    }


    SerDes2<MockObj> serdes = new SerDes2<BigTest.MockObj>() {
        public MockObj unmarshall(byte[] data) {
            ByteBuffer bb = ByteBuffer.wrap(data);
            long tstamp = bb.getLong();
            long idx = bb.getLong();
            return new MockObj(tstamp, idx);
        }

        public long timestampOfT(MockObj val) {
            return val.tstamp;
        }

        public byte[] marshall(MockObj val) {
            ByteBuffer bb = ByteBuffer.allocate(MockObj.objSize);
            bb.putLong(val.tstamp);
            bb.putLong(val.idx1);
            byte[] data = bb.array();
            return data;
        }
    };

    Function<MockObj, byte[]> keyer = val -> {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(val.idx1);
        byte[] data = bb.array();
        return data;
    };
}
