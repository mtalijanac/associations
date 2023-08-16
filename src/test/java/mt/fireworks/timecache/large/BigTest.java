package mt.fireworks.timecache.large;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.junit.Ignore;
import org.junit.Test;

import ch.qos.logback.core.joran.conditional.ThenAction;
import ch.qos.logback.core.recovery.ResilientSyslogOutputStream;
import lombok.AllArgsConstructor;
import lombok.Data;
import mt.fireworks.timecache.*;

public class BigTest {

    @Data @AllArgsConstructor
    static class MockObj {
        final static int objSize = 2 * 8;

        long tstamp;
        long idx1;
        byte[] randomData;
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        BigTest bt = new BigTest();
        bt.runLargeTest();
    }

    static class Threads implements ThreadFactory {
        String name = "Worker-";
        int counter = 0;

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(name + (++counter));
            return t;
        }
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

        BytesKeyedCacheFactory<MockObj> fact = new BytesKeyedCacheFactory<>();
        fact.setSerdes(serdes);
        fact.addKeyer("IDX1", keyer);

        long start = System.currentTimeMillis();
        fact.setStartTimeMillis(start);

        long tenMinutes = 10 * 60 * 1000;
        fact.storageConf(7, 1, tenMinutes);

        BytesKeyedCache<MockObj> cache = fact.getInstance();

        BlockingQueue<MockObj> q = new ArrayBlockingQueue<>(1000);

        ExecutorService executors = Executors.newFixedThreadPool(20, new Threads());
        ArrayList<Future<Long>> futures = new ArrayList<>();
        AtomicBoolean end = new AtomicBoolean(false);

        int workerCount = 3;
        for (int idx = 0; idx < workerCount; idx++) {
            Future<Long> fut = executors.submit(new Worker(cache, q, end));
            futures.add(fut);
        }

        long t = -System.currentTimeMillis();

        ThreadLocalRandom rng = ThreadLocalRandom.current();
        for (int d = 0; d < 110; d++) {
            // one day
            for (int c = 0; c < 1_000_000; c++) {
                long tstamp = rng.nextLong(start, start + tenMinutes);

                int i = rng.nextInt(indexes.length);
                long index = indexes[i];

                int len = rng.nextInt(280, 320);
                byte[] data = new byte[len];
                rng.nextBytes(data);

                MockObj obj = new MockObj(tstamp, index, data);
                q.put(obj);
            }
            cache.tick();
            start = start + tenMinutes;
            if (d == 9 || d == 0) {
                System.out.println(cache.allMetrics());

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

        System.out.println(cache.allMetrics());
    }

    @AllArgsConstructor
    static class Worker implements Callable<Long> {
        BytesKeyedCache<MockObj> cache;
        BlockingQueue<MockObj> q;
        AtomicBoolean end;

        public Long call() throws Exception {
            try {
            System.out.println(Thread.currentThread().getName() + " called");
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
            catch (Exception ex) {
                System.out.println(ex);
                throw ex;
            }
        }
    }



    long[] testIndexes() {
        int indexSize = 100_000;
        long[] indexes = new long[indexSize];
        for (int i = 0; i < indexes.length; i++)
            indexes[i] = ThreadLocalRandom.current().nextLong();
        return indexes;
    }


    SerDes<MockObj> serdes = new SerDes<BigTest.MockObj>() {
        public MockObj unmarshall(byte[] data) {
            ByteBuffer bb = ByteBuffer.wrap(data);
            long tstamp = bb.getLong();
            long idx = bb.getLong();
            short len = bb.getShort();
            byte[] objData = new byte[len];
            bb.get(objData);
            return new MockObj(tstamp, idx, objData);
        }

        public MockObj unmarshall(byte[] data, int position, int length) {
            ByteBuffer bb = ByteBuffer.wrap(data, position, length);
            long tstamp = bb.getLong();
            long idx = bb.getLong();
            short len = bb.getShort();
            byte[] objData = new byte[len];
            bb.get(objData);
            return new MockObj(tstamp, idx, objData);
        }

        public long timestampOfT(MockObj val) {
            return val.tstamp;
        }

        public byte[] marshall(MockObj val) {
            ByteBuffer bb = ByteBuffer.allocate(MockObj.objSize + val.randomData.length + 2);
            bb.putLong(val.tstamp);
            bb.putLong(val.idx1);
            bb.putShort((short) val.randomData.length);
            bb.put(val.randomData);
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
