package mt.fireworks.timecache.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import mt.fireworks.timecache.storage.ByteList.ForEachAction;

public class ByteListTest {


    byte[] randomData(int min, int max) {
        int len = ThreadLocalRandom.current().nextInt(min, max);
        byte[] data = new byte[len];
        ThreadLocalRandom.current().nextBytes(data);
        return data;
    }


    @Test
    public void usageExample() {
        // prepare some data
        byte[] someData_1 = randomData(250, 500);
        ByteBuffer.wrap(someData_1).putInt(0x5ca1ab1e);

        byte[] someData_2 = randomData(250, 500);
        ByteBuffer.wrap(someData_2).putInt(0xCAFEBABE);


        // init list:
        ByteList byteList = new ByteList();


        // write data:
        long dataKey_1 = byteList.add(someData_1);
        long dataKey_2 = byteList.add(someData_2);


        // fetch length of data under key:
        int dataLen_1 = byteList.length(dataKey_1);
        assertEquals(someData_1.length, dataLen_1);


        // read data under key:
        byte[] copyOfSomeData_1 = byteList.get(dataKey_1);
        assertArrayEquals(someData_1, copyOfSomeData_1);


        // copy data to custom a destination:
        byte[] destination = new byte[1000];
        int at = 125;
        byteList.copy(dataKey_2, destination, at);
        assertEquals(0, Arrays.compare(someData_2, 0, someData_2.length, destination, at, someData_2.length + at));


        // peek example, return int at start of data:
        int cafeBabe = byteList.peek(dataKey_2, (objPos, bucket, pos, len) -> {
            return ByteBuffer.wrap(bucket).getInt(pos);
        });
        assertEquals(0xCAFEBABE, cafeBabe);


        // forEach example, count objects:
        AtomicInteger objCount = new AtomicInteger();
        byteList.forEach((objPos, bucket, pos, len) -> {
            objCount.incrementAndGet();
            return ForEachAction.CONTINUE;
        });
        assertEquals(2, objCount.get());
    }



    @Test
    public void addALotOfData() {
        ByteList bl = new ByteList();

        int data_written = 0;
        int data_read = 0;

        byte[] dest = new byte[1500];

        for (int i = 0; i < 100_000; i++) {
            byte[] data = randomData(250, 1500);
            long key = bl.add(data);
            data_written += data.length;

            // test length of data
            int len = bl.length(key);
            assertEquals(data.length, len);

            // test reading
            byte[] readData = bl.get(key);
            data_read += readData.length;
            Assert.assertArrayEquals(data, readData);

            // test copying
            int count = bl.copy(key, dest, 0);
            assertEquals(data.length, count);
            for (int idx = 0; idx < data.length; idx++) {
                assertEquals(data[idx], dest[idx]);
            }
        }
        assertEquals(data_written, data_read);

        // test foreach:
        AtomicInteger objCount = new AtomicInteger();
        AtomicInteger objLen = new AtomicInteger();
        bl.forEach((objPos, bucket, pos, len) -> {
            objCount.incrementAndGet();
            objLen.addAndGet(len);
            return ForEachAction.CONTINUE;
        });

        assertEquals(100_000, objCount.get());
        assertEquals(data_written, objLen.get());
    }

}
