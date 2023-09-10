package mt.fireworks.associations;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.text.DecimalFormat;
import java.util.Random;

public class ByteListBenchmark {


//    int min_data_size = 1024;
//    int max_data_size = 0xFFFF;
    int min_data_size = 2600;
    int max_data_size = 32000;
    long time_x = 10;
    long sleep_start = 5 * time_x;
    long sleep_after_benchmark = 20 * time_x;
    long limit = 1024 * 1024 * 1024;

    public static void main(String[] args) throws InterruptedException {
        ByteListBenchmark blb = new ByteListBenchmark();
        Thread.sleep(blb.sleep_start);
        blb.benchmark(blb.limit);
        System.gc();
        blb.benchmark(blb.limit);
    }

    Random rng = new Random();

    byte[] randomData(int min, int max) {
        int len = rng.nextInt(max-min) + min;
        byte[] data = new byte[len];
        rng.nextBytes(data);
        return data;
    }


     void benchmark(final long limit) throws InterruptedException {
        ByteList bl = new ByteList(32 * 1024 * 1024);

        // generate data
        int data_set_size = 1000;
        byte[][] data = new byte[data_set_size][];
        for (int i = 0; i < data.length; i++) {
            data[i] = randomData(min_data_size, max_data_size);
        }

        byte[] copy_data = new byte[max_data_size + 1];

        long data_written = 0;
        long write_count = 0;
        long twrite = 0;
        long tread = 0;
        long tcopy = 0;


        while(data_written < limit) {
            int dIdx = rng.nextInt(data_set_size);

            byte[] d = data[dIdx];

            long t1 = System.nanoTime();
            long key = bl.add(d);
            long t2 = System.nanoTime();
            byte[] dread = bl.get(key);
            long t3 = System.nanoTime();
            int copy_len = bl.copy(key, copy_data, 0);
            long t4 = System.nanoTime();

            twrite += (t2 - t1);
            tread += (t3-t2);
            tcopy += (t4-t3);

            assertArrayEquals(d, dread);
            assertEquals(d.length, copy_len);
            data_written += d.length;
            write_count += 1;
        }

        DecimalFormat df = new DecimalFormat("0.00");

        System.out.println("Data written: " + data_written + " byts");
        long storageSize = bl.size.get();
        double efficency = (double) storageSize / data_written;
        System.out.println("Storage size: " + storageSize + " bytes");
        System.out.println("Efficency:    " + df.format(efficency));
        int bucketCount = bl.buckets.size();
        long bucketSize = bl.bucketSize * bucketCount;
        double efficency2 = (double) bucketSize / data_written;
        System.out.println("bucketSize:   " + bucketSize + " bytes");
        System.out.println("Efficency 2:  " + df.format(efficency2));


        System.out.println("write count:  " + write_count);
        System.out.println("twrite:       " + twrite + " ns");
        System.out.println("tread:        " + tread  + " ns");
        System.out.println("tcopy:        " + tcopy  + " ns");


        // write speed:
        double write_speed = (double) data_written * 1_000_000_000d / twrite;
        System.out.println("Write speed:  " + df.format(write_speed) + " bytes/sec");

        double read_speed = (double) data_written * 1_000_000_000d / tread;
        System.out.println("Read speed:   " + df.format(read_speed) + " bytes/sec");

        double copy_speed = (double) data_written * 1_000_000_000d / tcopy;
        System.out.println("Copy speed:   " + df.format(copy_speed) + " bytes/sec");


        System.out.println();

        Thread.sleep(sleep_after_benchmark);
    }


}
