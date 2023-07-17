package mt.fireworks.timecache.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public class ByteList {

    static class Conf {
        /** Initial number of buckets in window **/
        int initialNumberOfBucketsInWindow = 1;

        /** bucket size in bytes */
        int bucketSize = 1024 * 1024;

        /** size of data header in bytes, where len of data is stored */
        int dataHeaderSize = 2;

        /** size of padding alignment in bytes */
        int dataPadding = 0;

        byte padding = (byte) 0xEF;
    }

    @FunctionalInterface
    interface Peeker<T> {
        T peek(byte[] bucket, int pos, int len);
    }

    AtomicLong size = new AtomicLong();
    Conf conf = new Conf();
    ArrayList<byte[]> buckets = new ArrayList<byte[]>();

    public ByteList() {
        buckets.add(new byte[conf.bucketSize]);
    }


    long size() {
        return size.get();
    }


    /**
     * Write data to storage, and return index where is written.
     * Index points to data header.
     */
    long add(byte[] data) {
        // calculate data size
        final int dataSize = conf.dataHeaderSize + data.length;

        final int paddingSize = conf.dataPadding == 0
                 ? 0
                 : conf.dataPadding - (dataSize % conf.dataPadding);

        final int totalDataSize = dataSize + paddingSize; // size of data to be written

        // allocate memory if needed
        {
            long leadSize = (buckets.size() - 1l) * conf.bucketSize;
            int bucketPos = (int) (size.get() - leadSize);
            int spaceLeft = conf.bucketSize - bucketPos;
            if (spaceLeft < totalDataSize) {
                size.set((long) buckets.size() * conf.bucketSize);
                buckets.add(new byte[conf.bucketSize]);
            }
        }

        // write data at position
        final long writePos = size.getAndAdd(totalDataSize);
        {
            byte[] bucket = buckets.get(buckets.size() - 1);
            long leadSize = (buckets.size() - 1l) * conf.bucketSize;

            int offset = (int) (writePos - leadSize);
            int dataOffset =  offset + conf.dataHeaderSize;
            int paddingOffset = dataOffset + data.length;
            short dataLength = BitsAndBytes.toUnsignedShort(data.length);

            // write header, data, and padding
            BitsAndBytes.writeShort(dataLength, bucket, offset);
            System.arraycopy(data, 0, bucket, dataOffset, data.length);
            if (paddingSize > 0) {
                Arrays.fill(bucket, paddingOffset, paddingOffset + paddingSize, conf.padding);
            }
        }

        return writePos;
    }

    /** peek for data under key */
    <T> T peek(long key, Peeker<T> peeker) {
        int bucketIndex = (int) (key / conf.bucketSize);
        int readPos = (int) (key - (long) bucketIndex * conf.bucketSize);
        int dataPos = readPos + conf.dataHeaderSize;
        byte[] bucket = buckets.get(bucketIndex);
        int dataLen = BitsAndBytes.readUnsignedShort(bucket, readPos);
        T res = peeker.peek(bucket, dataPos, dataLen);
        return res;
    }

    /** @return Length of data stored under key. */
    int length(long key) {
        return peek(key, (bucket, pos, len) -> len);
    }


    /** @return data under key */
    byte[] get(long key) {
        byte[] data = peek(key, (bucket, pos, len) -> {
            byte[] res = new byte[len];
            System.arraycopy(bucket, pos, res, 0, len);
            return res;
        });
        return data;
    }

    /** Copy data under key to dest array at given idx.
     * @throws RuntimeException when there is no space in destination */
    int copy(long key, byte[] dest, int destPos) {
        return peek(key, (bucket, pos, len) -> {
            int space = dest.length - destPos;
            if (space < len) {
                throw new RuntimeException("No enough space in destination to copy data. Data len is: " + len + ", but space is: " + space);
            }
            System.arraycopy(bucket, pos, dest, destPos, len);
            return len;
        });
    }

    enum ForEachAction {
        CONTINUE, BREAK
    }

    void forEach(Peeker<ForEachAction> userPeeker) {
        Peeker<Integer> iterator = new Peeker<Integer>() {
            public Integer peek(byte[] bucket, int pos, int len) {
                ForEachAction res = userPeeker.peek(bucket, pos, len);
                if (ForEachAction.BREAK.equals(res)) {
                    return -1;
                }

                final int dataSize = conf.dataHeaderSize + len;
                final int paddingSize = conf.dataPadding == 0 ? 0
                        : conf.dataPadding - (dataSize % conf.dataPadding);
                final int totalDataSize = dataSize + paddingSize; // size of data to be written
                final int nextPos = pos + totalDataSize - conf.dataHeaderSize;
                return nextPos;
            }
        };


        for (int bucketIndex = 0; bucketIndex < buckets.size(); bucketIndex++) {
            byte[] bucket = buckets.get(bucketIndex);
            int dataLen = BitsAndBytes.readUnsignedShort(bucket, 0);
            int dataPos = conf.dataHeaderSize;

            inBucket: while (true) {
                int nextObjPos = iterator.peek(bucket, dataPos, dataLen).intValue();
                if (nextObjPos < 0) {
                    return;
                }
                if (nextObjPos >= bucket.length) {
                    break inBucket;
                }
                dataLen = BitsAndBytes.readUnsignedShort(bucket, nextObjPos);
                if (dataLen == 0) {
                    break inBucket;
                }
                dataPos = nextObjPos + conf.dataHeaderSize;
            }
        }
    }

}
