package mt.fireworks.timecache;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Ignore;
import org.junit.Test;

import lombok.AllArgsConstructor;
import mt.fireworks.timecache.storage.ByteCacheFactory;
import mt.fireworks.timecache.storage.ByteCacheImpl;

public class BigTest2 {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        BigTest2 bt = new BigTest2();
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

        long[] randomValues = rngValues();

        ByteCacheFactory<MockObj> fact = new ByteCacheFactory<>();
        fact.setSerdes(serdes);
        fact.addKeyers(keyer);

        long start = System.currentTimeMillis();
        fact.setStartTimestamp(start);

        long tenMinutes = 10 * 60 * 1000;
        fact.storageConf(7, 1, tenMinutes);

        ByteCacheImpl<MockObj> cache = fact.getInstance();

        BlockingQueue<MockObj> q = new LinkedBlockingDeque<>();

        ExecutorService executors = Executors.newCachedThreadPool();
        ArrayList<Future<Long>> futures = new ArrayList<>();
        AtomicBoolean end = new AtomicBoolean(false);

        int workerCount = 1;
        for (int idx = 0; idx < workerCount; idx++) {
            Future<Long> fut = executors.submit(new Worker(cache, q, end));
            futures.add(fut);
        }

        AtomicLong counter = new AtomicLong();
        long t = -System.currentTimeMillis();

        for (int d = -5; d < 15; d++) {
            if (d == 0) {
                System.out.println(d + " - " + cache.toString());
                t = -System.currentTimeMillis();
                counter.set(0);
            }

            int producerCount = 2_00_000 / 3;
            long windowEnd = start + tenMinutes;
            ArrayList<Future<Long>> fproduces = new ArrayList<>();
            for (int idx = 0; idx < 3; idx++) {
                Producer p = new Producer(producerCount, start, windowEnd, randomValues, q);
                Future<Long> future = executors.submit(p);
                fproduces.add(future);
            }

            for (Future<Long> f : fproduces) {
                Long val = f.get();
                counter.addAndGet(val);
            }

            /*
            // one day
            for (int c = 0; c < 2_00_000; c++) {
                long tstamp = ThreadLocalRandom.current().nextLong(start, start + tenMinutes);

                int i = ThreadLocalRandom.current().nextInt(randomValues.length);
                long val = randomValues[i];

                int tekstLen = ThreadLocalRandom.current().nextInt(240, 300);
                String tekst = RandomStringUtils.randomAlphanumeric(tekstLen);

                MockObj obj = new MockObj(tstamp, val, tekst);
                q.put(obj);
                counter.incrementAndGet();
            }
            */
            cache.tick();
            start = start + tenMinutes;
        }

        System.out.println("Counter: " + counter.get());

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
    static class Producer implements Callable<Long> {
        long objCount;
        long start, end;
        long[] randomValues;
        BlockingQueue<MockObj> q;


        @Override
        public Long call() throws Exception {
            long tstamp = ThreadLocalRandom.current().nextLong(start, end);

            long counter = 0;
            for (; counter < objCount; counter++) {
                int i = ThreadLocalRandom.current().nextInt(randomValues.length);
                long val = randomValues[i];

                int tekstLen = ThreadLocalRandom.current().nextInt(240, 300);
                String tekst = RandomStringUtils.randomAlphanumeric(tekstLen);

                MockObj obj = new MockObj(tstamp, val, tekst);
                q.put(obj);
            }

            return counter;
        }

    }

    @AllArgsConstructor
    static class Worker implements Callable<Long> {
        ByteCacheImpl<MockObj> cache;
        BlockingQueue<MockObj> q;
        AtomicBoolean end;

        public Long call() throws Exception {
            try {

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
//                    Object[] res = cache.getArray(val);
                    List<Entry<byte[], List<MockObj>>> res = cache.get(val);
                    ArrayList<MockObj> objects = (ArrayList<MockObj>) res.get(0).getValue();
                    if (objects.size() > 1000) {
                        System.out.println(objects.size());
                    }
                }
                System.out.println("Worker '" + Thread.currentThread().getName() + "' ended. Read: " + count.get());
                return count.get();
            }
            catch (Throwable ex) {
                System.err.println(ex);
                throw ex;
            }
        }
    }



    long[] rngValues() {
        int count = 100_000;
        long[] vals = new long[count];
        for (int i = 0; i < vals.length; i++)
            vals[i] = ThreadLocalRandom.current().nextLong();
        return vals;
    }


    SerDes<MockObj> serdes = new SerDes<MockObj>() {
        public MockObj unmarshall(byte[] data) {
            ByteBuffer bb = ByteBuffer.wrap(data);
            long tstamp = bb.getLong();
            long val = bb.getLong();
            byte[] tekstData = new byte[data.length - MockObj.objSize];
            System.arraycopy(data, 16, tekstData, 0, tekstData.length);
            return new MockObj(tstamp, val, new String(tekstData, StandardCharsets.UTF_8));
        }

        public long timestampOfT(MockObj val) {
            return val.getTstamp();
        }

        public byte[] marshall(MockObj val) {
            byte[] bytes = val.getTekst().getBytes(StandardCharsets.UTF_8);
            ByteBuffer bb = ByteBuffer.allocate(bytes.length + MockObj.objSize);
            bb.putLong(val.getTstamp());
            bb.putLong(val.getValue());
            bb.put(bytes);
            byte[] data = bb.array();
            return data;
        }
    };

    Function<MockObj, byte[]> keyer = val -> {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(val.getValue());
        byte[] data = bb.array();
        return data;
    };
}
