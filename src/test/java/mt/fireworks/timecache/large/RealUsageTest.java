package mt.fireworks.timecache.large;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.commons.lang3.RandomStringUtils;

import lombok.AllArgsConstructor;
import lombok.Data;
import mt.fireworks.associations.cache.*;

public class RealUsageTest {

    @Data
    static class Trx {
        long tstamp;
        long amount;
        String pan;
        String merId;
        String trxData;
    }

    static class TrxSerDes implements SerDes<Trx> {

        public byte[] marshall(Trx trx) {
            ByteBuffer bb = ByteBuffer.allocate(1000);
            bb.clear();
            bb.putLong(trx.tstamp);
            bb.putLong(trx.amount);

            bb.put((byte) trx.pan.length());
            bb.put(trx.pan.getBytes(UTF_8));

            bb.put((byte) trx.merId.length());
            bb.put(trx.merId.getBytes(UTF_8));

            bb.putShort((short) trx.trxData.length());
            bb.put(trx.trxData.getBytes(UTF_8));

            bb.flip();
            byte[] data = new byte[bb.limit()];
            bb.get(data);
            return data;
        }

        public Trx unmarshall(byte[] data) {
            return unmarshall(data, 0, data.length);
        }

        public Trx unmarshall(byte[] data, int position, int length) {
            ByteBuffer bb = ByteBuffer.wrap(data, position, length);

            Trx trx = new Trx();
            trx.setTstamp(bb.getLong());
            trx.setAmount(bb.getLong());

            int panLen = bb.get();
            String pan = new String(data, bb.position(), panLen, UTF_8);
            trx.setPan(pan);
            bb.position(bb.position() + panLen);

            int merIdLen = bb.get();
            String merId = new String(data, bb.position(), merIdLen, UTF_8);
            trx.setMerId(merId);
            bb.position(bb.position() + merIdLen);

            int trxDataLen = bb.getShort();
            try {
                String trxData = new String(data, bb.position(), trxDataLen, UTF_8);
                trx.setTrxData(trxData);
            }
            catch (Exception e) {
                System.out.println("Data len: " + data.length + " position: " + bb.position() + " trxDataLen: " + trxDataLen);
                throw e;
            }
            bb.position(bb.position() + merIdLen);

            return trx;
        }


        public long timestampOfT(Trx val) {
            return val.tstamp;
        }
    }

    @Data @AllArgsConstructor
    static class Threads implements ThreadFactory {
        String name;
        int count;
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName(name + "-" + (++count));
            return thread;
        }
    }

    void run() throws InterruptedException {
        long tenSec = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
        long fiveSec = TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS);
        long iterDur = fiveSec;
        int workerCount = 1;

        Function<Trx, byte[]> panIndex = (Trx t) -> t.getPan().getBytes(UTF_8);
        Function<Trx, byte[]> midIndex = (Trx t) -> t.getMerId().getBytes(UTF_8);

        BytesKeyedCacheFactory<Trx> factory = new BytesKeyedCacheFactory<>();
        factory.setSerdes(new TrxSerDes());
        factory.addKeyer("PAN", panIndex);
        factory.addKeyer("MID", midIndex);
        factory.setHistoryWindowsCount(7);
        factory.setFutureWindowCount(1);
        factory.setWindowTimespanMs(iterDur);
        factory.setWinCapacity(8 * 8 * 1024 * 1024);

        BytesCache<Trx> cache = factory.getInstance();

        ArrayList<String> pans = new ArrayList<>();
        for (int i = 0; i < 200_000; i++) {
            String pan = RandomStringUtils.randomNumeric(16);
            pans.add(pan);
        }

        ArrayList<String> mids = new ArrayList<>();
        for (int i = 0; i < 20_000; i++) {
            String mid = RandomStringUtils.randomAlphanumeric(6,12);
            mids.add(mid);
        }

        ArrayList<String> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String trxData = RandomStringUtils.randomAlphanumeric(280, 320);
            data.add(trxData);
        }


        AtomicBoolean programEnd = new AtomicBoolean(false);
        ExecutorService executors = Executors.newCachedThreadPool(new Threads("producers", 0));
        ArrayList<Producer> producers = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            Producer producer = new Producer("PRODUCER_" + i, cache, pans, mids, data, programEnd);
            producers.add(producer);
            executors.submit(producer);
        }

        long sleepTime = iterDur;
        for (int i = 0; i < 15; i++) {
            Thread.sleep(sleepTime);
            long dur = -System.nanoTime();
            cache.tick();
            System.out.println("~-~~-~~-~~-~~-~~-~~-~~-~~-~~-~~-~~-~~-~~-~~-~~-~~-~~-~~-~~-~");
            System.out.println("Iteration: " + i);
            for (Producer producer: producers) {
                System.out.println(producer);
            }
            System.out.println(cache.allMetrics());
            dur += System.nanoTime();
            sleepTime = iterDur - Math.round(dur / 1_000_000);
        }

        programEnd.set(true);
        executors.shutdown();
    }





    @Data @AllArgsConstructor
    static class Producer implements Callable<Long> {
        String name;
        BytesCache<Trx> cache;
        ArrayList<String> pans;
        ArrayList<String> mids;
        ArrayList<String> data;

        AtomicBoolean end;

        final AtomicLong trxCounter = new AtomicLong();
        final AtomicLong panCacheCounter = new AtomicLong();
        final AtomicLong midCacheCounter = new AtomicLong();

        @Override
        public String toString() {
            return name + " - trxCount: " + trxCounter.get()
                        + ", panCache: " + panCacheCounter.get()
                        + ", midCache: " + midCacheCounter.get();
        }

        @Override
        public Long call() throws Exception {

            while (!end.get()) {
                Trx trx = randomTrx();
                cache.add(trx);

                Map<String, List<Trx>> map = cache.getAsMap(trx);

                List<Trx> panCache = map.get("PAN");
                if (panCache != null) panCacheCounter.addAndGet(panCache.size());

                List<Trx> midCache = map.get("MID");
                if (midCache != null) midCacheCounter.addAndGet(midCache.size());

                trxCounter.incrementAndGet();
            }
            return trxCounter.get();
        }

        Trx randomTrx() {
            ThreadLocalRandom rng = ThreadLocalRandom.current();

            int panIdx = rng.nextInt(pans.size());
            String pan = pans.get(panIdx);

            int midIdx = rng.nextInt(mids.size());
            String mid = mids.get(midIdx);

            int dataIdx = rng.nextInt(data.size());
            String txt = data.get(dataIdx);

            long amount = rng.nextLong(10_000);
            long tstamp = System.currentTimeMillis();

            Trx trx = new Trx();
            trx.setPan(pan);
            trx.setMerId(mid);
            trx.setAmount(amount);
            trx.setTstamp(tstamp);
            trx.setTrxData(txt);
            return trx;
        }
    }



    public static void main(String[] args) throws InterruptedException {
        RealUsageTest test = new RealUsageTest();
        for (int i = 0; i < 10; i++) {
            test.run();
            System.out.println("Ding");
            System.out.println("Ding");
            System.out.println("Ding");
            System.out.println("Ding");
            System.out.println("Ding");
        }
    }

}
