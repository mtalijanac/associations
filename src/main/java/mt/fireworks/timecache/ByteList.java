package mt.fireworks.timecache;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class ByteList {

    static class Conf {
        /** Initial number of buckets in window **/
        int initialNumberOfBucketsInWindow = 1;

        /** bucket size in bytes */
        int bucketSize = 1024 * 1024;

        /** size of data header in bytes, where len of data is stored */
        int dataHeaderSize = 2;
    }

    @FunctionalInterface
    interface Peeker<T> {
        T peek(long objPos, byte[] bucket, int pos, int len);
    }




    AtomicLong size = new AtomicLong();
    Conf conf = new Conf();
    ArrayList<byte[]> buckets = new ArrayList<byte[]>();

    public ByteList() {
        buckets.add(new byte[conf.bucketSize]);
    }

    public ByteList(int bucketSizeInBytes, int dataHeaderSize) {
        this.conf.bucketSize = bucketSizeInBytes;
        this.conf.dataHeaderSize = dataHeaderSize;
        buckets.add(new byte[conf.bucketSize]);
    }


    /**
     * Write data to storage, and return index where is written.
     * Index points to data header.
     */
    // FIXME ovaj syncronized je usko grlo
    public long add(byte[] data) {
        short dataLength;
        int offset, dataOffset;
        byte[] bucket;
        long writePos;

        final int dataSize = conf.dataHeaderSize + data.length;

        synchronized (this) {
            // allocate memory if needed
            {
                long leadSize = (buckets.size() - 1l) * conf.bucketSize;
                int bucketPos = (int) (size.get() - leadSize);
                int spaceLeft = conf.bucketSize - bucketPos;
                if (spaceLeft < dataSize) {
                    size.set((long) buckets.size() * conf.bucketSize);
                    buckets.add(new byte[conf.bucketSize]);
                }
            }

            // write data at position
            {
                writePos = size.getAndAdd(dataSize);
                bucket = buckets.get(buckets.size() - 1);
                long leadSize = (buckets.size() - 1l) * conf.bucketSize;

                offset = (int) (writePos - leadSize);
                dataOffset =  offset + conf.dataHeaderSize;
                dataLength = BitsAndBytes.toUnsignedShort(data.length);
            }
        }

        // write header, data

        BitsAndBytes.writeShort(dataLength, bucket, offset);
        System.arraycopy(data, 0, bucket, dataOffset, data.length);
        return writePos;
    }

    /** peek for data under key */
    <T> T peek(long key, Peeker<T> peeker) {
        int bucketIndex = (int) (key / conf.bucketSize);
        int readPos = (int) (key - (long) bucketIndex * conf.bucketSize);
        int dataPos = readPos + conf.dataHeaderSize;
        byte[] bucket = buckets.get(bucketIndex);
        int dataLen = BitsAndBytes.readUnsignedShort(bucket, readPos);
        T res = peeker.peek(key, bucket, dataPos, dataLen);
        return res;
    }

    /** @return Length of data stored under key. */
    int length(long key) {
        return peek(key, (objPos, bucket, pos, len) -> len);
    }


    /** @return data under key */
    public byte[] get(long key) {
        byte[] data = peek(key, (objPos, bucket, pos, len) -> {
            byte[] res = new byte[len];
            System.arraycopy(bucket, pos, res, 0, len);
            return res;
        });
        return data;
    }

    /** Copy data under key to dest array at given idx.
     * @throws RuntimeException when there is no space in destination */
    public int copy(long key, byte[] dest, int destPos) {
        return peek(key, (objPos, bucket, pos, len) -> {
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

    public void forEach(Peeker<ForEachAction> userPeeker) {
        Peeker<Integer> iterator = new Peeker<Integer>() {
            public Integer peek(long objPos, byte[] bucket, int pos, int len) {
                ForEachAction res = userPeeker.peek(objPos, bucket, pos, len);
                if (ForEachAction.BREAK.equals(res)) {
                    return -1;
                }

                final int dataSize = conf.dataHeaderSize + len;
                final int nextPos = pos + dataSize - conf.dataHeaderSize;
                return nextPos;
            }
        };


        for (int bucketIndex = 0; bucketIndex < buckets.size(); bucketIndex++) {
            byte[] bucket = buckets.get(bucketIndex);
            int dataLen = BitsAndBytes.readUnsignedShort(bucket, 0);
            if (dataLen == 0) continue;

            int dataPos = conf.dataHeaderSize;

            long lead = conf.bucketSize * (bucketIndex == 0 ? 0l : bucketIndex - 1l) ;

            inBucket: while (true) {
                long objPos = lead + dataPos - conf.dataHeaderSize;
                int nextObjPos = iterator.peek(objPos, bucket, dataPos, dataLen).intValue();
                if (nextObjPos < 0) {
                    return;
                }
                if (nextObjPos >= bucket.length - conf.dataHeaderSize) {
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
