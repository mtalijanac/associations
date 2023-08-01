package mt.fireworks.timecache;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.junit.Ignore;
import org.junit.Test;

import lombok.AllArgsConstructor;
import lombok.Data;
import mt.fireworks.timecache.storage.ByteCacheFactory;
import mt.fireworks.timecache.storage.ByteCacheImpl;

public class BigTest {

    @Data @AllArgsConstructor
    static class MockObj {
        final static int objSize = 2 * 8;

        long tstamp;
        long idx1;
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        BigTest bt = new BigTest();
        bt.runLargeTest();
    }

    /**
     * Store large amount of data in this single thread test.
     * Window size: 10 min day
     * expected window store count: 1 000 000 objects
     * Cache size: 7 history windows, 1 future window
     * Test run time: 30 windows
     * Index count: 1
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Ignore
    @Test
    public void runLargeTest() throws InterruptedException, ExecutionException {
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

        BlockingQueue<MockObj> q = new ArrayBlockingQueue<>(1000);

        ExecutorService executors = Executors.newFixedThreadPool(20);
        ArrayList<Future<Long>> futures = new ArrayList<>();
        AtomicBoolean end = new AtomicBoolean(false);

        int workerCount = 3;
        for (int idx = 0; idx < workerCount; idx++) {
            Future<Long> fut = executors.submit(new Worker(cache, q, end));
            futures.add(fut);
        }

        AtomicLong counter = new AtomicLong();
        long t = -System.currentTimeMillis();

        for (int d = 0; d < 110; d++) {
            // one day
            for (int c = 0; c < 1_000_000; c++) {
                long tstamp = ThreadLocalRandom.current().nextLong(start, start + tenMinutes);

                int i = ThreadLocalRandom.current().nextInt(indexes.length);
                long index = indexes[i];

                MockObj obj = new MockObj(tstamp, index);
                q.put(obj);

//                if (counter.incrementAndGet() % 1_000_000 == 0)
//                    System.out.println("Put: " + counter.get());
            }
            cache.tick();
            start = start + tenMinutes;
            if (d == 9 || d == 0) {
                System.out.println(d + " - " + cache.toString());
                t = -System.currentTimeMillis();
            }
        }


        executors.shutdown();
        end.set(true);

        System.out.println("Everyting submited");

        for (Future<Long> fut: futures) {
            Long res = fut.get();
        }

        t += System.currentTimeMillis();

        System.out.println(cache.toString());
        System.out.println("Duration: " + t + ", workerCount: " + workerCount);
        System.out.flush();
    }

    @AllArgsConstructor
    static class Worker implements Callable<Long> {
        ByteCacheImpl<MockObj> cache;
        BlockingQueue<MockObj> q;
        AtomicBoolean end;

        public Long call() throws Exception {
            AtomicLong count = new AtomicLong();
            while (true) {
                MockObj val = q.poll(2, TimeUnit.SECONDS);

                if (val == null) {
                    if (end.get())
                        break;
                    continue;
                }

                long c = count.incrementAndGet();
//                if (c % 100_000 == 0) System.out.println(c);
                cache.add(val);
            }
            System.out.println("Worker '" + Thread.currentThread().getName() + "' ended. Read: " + count.get());
            return count.get();
        }
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
