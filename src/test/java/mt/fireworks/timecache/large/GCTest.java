package mt.fireworks.timecache.large;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.Assert;

import lombok.AllArgsConstructor;
import lombok.Data;
import mt.fireworks.timecache.*;

/**
 * Test proper clearing of index.
 */
public class GCTest {

    BytesCache<Trx> cache;

    void run() {
        final long oneMinute    = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

        long now = System.currentTimeMillis();
        long windowDuration = oneMinute;
        int  pastWindowCount = 1;
        int  futureWindowCount = 1;

        BytesKeyedCacheFactory<Trx> fact = new BytesKeyedCacheFactory<>();
        fact.setSerdes(serdes);
        fact.addKeyer("PAN", keyer);
        fact.setHistoryWindowsCount(pastWindowCount);
        fact.setFutureWindowCount(futureWindowCount);
        fact.setWindowTimespanMs(windowDuration);
        fact.setStartTimeMillis(now);
        cache = fact.getInstance();

        int panRangeFrom = 1_000_000;
        int panRangeTo   = 2_000_000;

        long sumFirstDay  = addOneDay(cache, panRangeFrom, panRangeTo);
             sumFirstDay += addOneDay(cache, panRangeFrom, panRangeTo);
        long sum1 = amountSum(cache, panRangeFrom, panRangeTo);
        Assert.assertEquals(sumFirstDay, sum1);

        cache.tick();

        long sumSecondDay = addOneDay(cache, panRangeFrom, panRangeTo);
        long sum2 = amountSum(cache, panRangeFrom, panRangeTo);
        Assert.assertEquals(sumFirstDay + sumSecondDay, sum2);

        cache.tick();

        long sumThirdDay = addOneDay(cache, panRangeFrom, panRangeTo);
        long sum3 = amountSum(cache, panRangeFrom, panRangeTo);
        Assert.assertEquals(sumSecondDay + sumThirdDay, sum3);

        cache.tick();

        long sum4 = amountSum(cache, panRangeFrom, panRangeTo);
        Assert.assertEquals(sumThirdDay, sum4);

        cache.tick();

        long sum5 = amountSum(cache, panRangeFrom, panRangeTo);
        Assert.assertEquals(0, sum5);

        String val = cache.allMetrics();
        System.out.println(val);
    }


    long addOneDay(BytesCache<Trx> cache, int panRangeFrom, int panRangeTo) {
        long start = cache.startTimeMillis();
        long sum = 0;
        for (int i = panRangeFrom; i < panRangeTo; i++) {
            int amount = ThreadLocalRandom.current().nextInt(100);
            sum += amount;

            Trx trx = new Trx(start + 10, i, amount);
            cache.add(trx);
        }
        return sum;
    }

    long amountSum(BytesCache<Trx> cache, int panRangeFrom, int panRangeTo) {
        long sum = 0;
        long start = cache.startTimeMillis();
        Trx query = new Trx(start, 0, 0);
        for (int pan = panRangeFrom; pan < panRangeTo; pan++) {
            query.pan = pan;
            Map<String, List<Trx>> res = cache.getAsMap(query);
            List<Trx> trxes = res.get("PAN");
            if (trxes == null) continue;
            for(Trx t: trxes) sum += t.amount;
        }
        return sum;
    }


    @Data @AllArgsConstructor
    static class Trx {
        long tstamp;
        long pan;
        long amount;
    }


    SerDes<Trx> serdes = new SerDes<GCTest.Trx>() {
        public byte[] marshall(Trx val) {
            ByteBuffer bb = ByteBuffer.allocate(3*8);
            bb.putLong(val.getTstamp());
            bb.putLong(val.getPan());
            bb.putLong(val.getAmount());
            return bb.array();
        }

        public Trx unmarshall(byte[] data) {
            ByteBuffer wrap = ByteBuffer.wrap(data);
            return new Trx(wrap.getLong(), wrap.getLong(), wrap.getLong());
        }

        public long timestampOfT(Trx val) {
            return val.getTstamp();
        }
    };


    Function<Trx, byte[]> keyer = val -> {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(val.pan);
        byte[] data = bb.array();
        return data;
    };


    public static void main(String[] args) {
        GCTest gcTest = new GCTest();
        gcTest.run();
    }

}
