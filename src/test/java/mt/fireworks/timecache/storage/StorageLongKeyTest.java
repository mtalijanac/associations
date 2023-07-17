package mt.fireworks.timecache.storage;

import static org.junit.Assert.assertArrayEquals;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

import mt.fireworks.timecache.storage.StorageLongKey.Window;


public class StorageLongKeyTest {

    byte[] randomData(int min, int max) {
        int len = ThreadLocalRandom.current().nextInt(min, max);
        byte[] data = new byte[len];
        ThreadLocalRandom.current().nextBytes(data);
        return data;
    }

    @Test
    public void debug() {
        StorageLongKey storage = StorageLongKey.init();

        byte[] someData_1 = randomData(250, 500);
        ByteBuffer.wrap(someData_1).putInt(0x5ca1ab1e);
        long tstamp_1 = System.currentTimeMillis();

        long key1 = storage.addEntry(tstamp_1, someData_1);
        long index = storage.timeKeys.index(key1);

        byte[] readData_1 = storage.getEntry(key1);
        Assert.assertArrayEquals(someData_1, readData_1);
    }


    @Test
    public void usageExample() {
        // prepare some data
        byte[] someData_1 = randomData(250, 500);
        ByteBuffer.wrap(someData_1).putInt(0x5ca1ab1e);
        long tstamp_1 = System.currentTimeMillis() + 100;

        byte[] someData_2 = randomData(250, 500);
        ByteBuffer.wrap(someData_2).putInt(0xCAFEBABE);
        long tstamp_2 = System.currentTimeMillis() + 10000;


        // init storage
        StorageLongKey storage = StorageLongKey.init();


        // write data
        long key1 = storage.addEntry(tstamp_1, someData_1);
        long key2 = storage.addEntry(tstamp_2, someData_2);


        // read data:
        byte[] readData_1 = storage.getEntry(key1);
        Assert.assertArrayEquals(someData_1, readData_1);

        byte[] readData_2 = storage.getEntry(key2);
        Assert.assertArrayEquals(someData_2, readData_2);
    }

    @Test
    public void testWithLotOfData() {
        StorageLongKey storage = StorageLongKey.init();
        long[] timespan = storage.timespan();

        for (int i = 0; i < 1_000_000; i++) {
            long tstamp = ThreadLocalRandom.current().nextLong(timespan[0], timespan[1]);
            byte[] data = randomData(250, 1500);
            long key = storage.addEntry(tstamp, data);
            byte[] dataRead = storage.getEntry(key);
            assertArrayEquals(data, dataRead);
        }
    }

    @Test
    public void testWithLotOfData2() {
        StorageLongKey storage = StorageLongKey.init();
        long[] timespan = storage.timespan();

        long[] counter = new long[8];

        for (int i = 0; i < 2_000_000; i++) {
            long tstamp = System.currentTimeMillis();
            byte[] data = randomData(250, 320);
            long key = storage.addEntry(tstamp, data);
            byte[] dataRead = storage.getEntry(key);
            assertArrayEquals(data, dataRead);

            int winIdx = storage.windowIndexForTstamp(tstamp);
            counter[winIdx]++;
        }

        System.out.println(Arrays.toString(counter));
        for (Window w: storage.windows) {
            System.out.print(w.store.size.get() + ", ");
        }
    }

}
