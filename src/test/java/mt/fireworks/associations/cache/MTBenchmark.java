package mt.fireworks.associations.cache;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.commons.lang3.RandomStringUtils;

import mt.fireworks.associations.Associations;

public class MTBenchmark {

    final static int binLen = 6;
    final static int panLen = 16;

    final ArrayList<byte[]> mids = new ArrayList<>();
    final ArrayList<String> bins = new ArrayList<>();
    final ArrayList<byte[]> pans = new ArrayList<>();



    public void run() throws InterruptedException, ExecutionException {
        prepareData();

        Function<byte[], byte[]> pan = data -> {
            byte[] d = new byte[panLen];
            System.arraycopy(data, 8, d, 0, d.length);
            return d;
        };

        Function<byte[], byte[]> mid = data -> {
            byte len = data[8 + panLen];
            byte[] m = new byte[len];
            System.arraycopy(data, 8 + panLen + 1, m, 0, m.length);
            return m;
        };

        BytesCache<byte[]> cache = BytesCache.newInstance(byte[].class)
                .withSerdes(new BenchmarkSerdes())
                .associate("mid", mid)
                .associate("pan", pan)
                .historyWindowsCount(7)
                .futureWindowCount(1)
                .windowTimespan(1, TimeUnit.MINUTES)
                .indexMapCount(1)
                .enableMetrics()
                .build();

        final AtomicLong midCounter = new AtomicLong();
        final AtomicLong panCounter = new AtomicLong();
        final AtomicLong dataCounter = new AtomicLong();
        final AtomicLong dataSizeCounter = new AtomicLong();

        ExecutorService exectuor = Executors.newCachedThreadPool();
        ArrayList<Future<Long>> futures = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            Future<Long> task = exectuor.submit(new Callable<Long>() {
                public Long call() throws Exception {
                    final long start = System.currentTimeMillis();

                    long midCacheCounter = 0;
                    long panCacheCounter = 0;
                    long sizeCounter = 0;
                    long counter = 0;

                    while (true) endless_loop: {
                        times_up_test: {
                            long now = System.currentTimeMillis();
                            long t = now - start;
                            if (t > TimeUnit.SECONDS.toMillis(10)) {
                                break;
                            }
                        }

                        byte[] data = nextData();

                        Map<String, List<byte[]>> res = cache.addAndGet(data);
                        List<byte[]> midCache = res.get("mid");
                        List<byte[]> panCache = res.get("pan");

                        counter += 1;
                        sizeCounter += data.length;
                        midCacheCounter += midCache.size();
                        panCacheCounter += panCache.size();
                    }

                    final long end = System.currentTimeMillis();
                    long d = end - start;

                    float speed = (float) 1000f * counter / d;

                    String res = "dataCounter: " + counter + ", speed: " + speed + " entries per sec\n" +
                                 "dataSizeCounter: " + sizeCounter + " bytes\n" +
                                 "midCacheCounter: " + midCacheCounter + "\n" +
                                 "panCacheCounter: " + panCacheCounter + "\n\n";

                    System.out.println(res);

                    midCounter.addAndGet(midCacheCounter);
                    panCounter.addAndGet(panCacheCounter);
                    dataCounter.addAndGet(counter);
                    dataSizeCounter.addAndGet(sizeCounter);

                    return counter;
                }
            });
            futures.add(task);
        }

        for (Future<Long> f: futures) {
            f.get();
        }

        System.out.println(cache.allMetrics());

        exectuor.shutdown();
    }



    void prepareData() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        for (int i = 0; i < 30_000; i++) {
            String mid = RandomStringUtils.randomAlphanumeric(6, 16);
            byte[] bytes = mid.getBytes(StandardCharsets.US_ASCII);
            byte[] data = new byte[1 + bytes.length];
            data[0] = (byte) bytes.length;
            System.arraycopy(bytes, 0, data, 1, bytes.length);
            mids.add(data);
        }

        for (int i = 0; i < 10000; i++)  {
            String bin = RandomStringUtils.randomNumeric(binLen);
            bins.add(bin);
        }

        for (int i = 0; i < 1_000_000; i++) {
            String bin = bins.get(rng.nextInt(bins.size()));
            String pan = bin + RandomStringUtils.randomNumeric(panLen - binLen);
            byte[] bytes = pan.getBytes(StandardCharsets.US_ASCII);
            pans.add(bytes);
        }
    }




    byte[] nextData() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        byte[] data = null;

        randomData: {
            int len = rng.nextInt(280, 330);
            data = new byte[len];
            rng.nextBytes(data);
        }

        addTime: {
            long now = System.currentTimeMillis();
            byte [] bytes = ByteBuffer.allocate(8).putLong(now).array();
            System.arraycopy(bytes, 0, data, 0, 8);
        }

        addPan: {
            int idx = rng.nextInt(pans.size());
            byte[] pan = pans.get(idx);
            System.arraycopy(pan, 0, data, 8, panLen);
        }

        addMid: {
            int idx = rng.nextInt(mids.size());
            byte[] mid = mids.get(idx);
            System.arraycopy(mid, 0, data, 8 + panLen, mid.length);
        }

        return data;
    }


    static class BenchmarkSerdes extends Associations.BytesSerDes implements CacheSerDes<byte[]> {
        public long timestampOfT(byte[] val) {
            Buffer buf = ByteBuffer.allocate(8)
                                    .put(val, 0, 8)
                                    .flip();
            long tstamp = ((ByteBuffer) buf).getLong();
            return tstamp;
        }
    }


    public static void main(String[] args) throws InterruptedException, ExecutionException {
        MTBenchmark test = new MTBenchmark();
        test.run();
    }

}
