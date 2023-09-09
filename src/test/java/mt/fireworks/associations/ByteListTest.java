package mt.fireworks.associations;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import mt.fireworks.associations.BitsAndBytes;
import mt.fireworks.associations.ByteList;
import mt.fireworks.associations.ByteList.*;

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
        assertEquals(0, BitsAndBytes.compare(someData_2, 0, someData_2.length, destination, at, someData_2.length + at));


        // peek example, return int at start of data:
        int cafeBabe = byteList.peek(dataKey_2, (objPos, bucket, pos, len) -> {
            return ByteBuffer.wrap(bucket).getInt(pos);
        });
        assertEquals(0xCAFEBABE, cafeBabe);


        // forEach example, count objects:
        AtomicInteger objCount = new AtomicInteger();
        byteList.forEach((objPos, bucket, pos, len) -> objCount.incrementAndGet());
        assertEquals(2, objCount.get());
    }


    @Test
    public void addALotOfData() {
        ByteList bl = new ByteList();

        int data_written = 0;
        int data_read = 0;

        byte[] dest = new byte[1500];


        int N = 1_000_000;
        for (int i = 0; i < N; i++) {
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
            return null;
        });

        assertEquals(N, objCount.get());
        assertEquals(data_written, objLen.get());
    }


    @Test
    public void testDataIterator() {
        ArrayList<byte[]> randomData = new ArrayList<>();
        for (long len = 0; len < 1l * 1152 * 1152 * 1152;) {
            byte[] data = randomData(250, 500);
            randomData.add(data);
            len += data.length;
        }

        ByteList byteList = new ByteList();
        for (byte[] d: randomData)
            byteList.add(d);

        AtomicInteger counter = new AtomicInteger();
        DataIterator<byte[]> iterator = byteList.iterator(
            (objPos, bucket, pos, len) -> Arrays.copyOfRange(bucket, pos, pos + len)
        );

        while (iterator.hasNext()) {
            byte[] read = iterator.next();
            byte[] expected = randomData.get(counter.get());
            Assert.assertArrayEquals(expected, read);
            counter.incrementAndGet();
        }

        Assert.assertEquals(randomData.size(), counter.get());
    }

}
