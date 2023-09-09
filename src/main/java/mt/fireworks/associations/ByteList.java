package mt.fireworks.associations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import lombok.AllArgsConstructor;

/**
 * This list stores byte arrays in a form of [length of data, data].
 * For each stored array index into list is returned. Index is of long type
 * so max capacity of list is  {@code Long.MAX_VALUE} bytes, which is
 * for practical purposes unlimited.
 *
 * <p>Current storage limitation is 64kb size limit for stored array.
 * Default allocation size is 1 Mb, the value of it can be changed using
 * constructor.
 *
 */
public class ByteList {

    static class Conf {
        /** bucket size in bytes */
        int bucketSize = 1 * 1024 * 1024;

        /** size of data header in bytes, where len of data is stored */
        int dataHeaderSize = 2;
    }

    @FunctionalInterface
    public interface Peeker<T> {
        T peek(long objPos, byte[] bucket, int pos, int len);
    }


    AtomicLong size = new AtomicLong();
    Conf conf = new Conf();
    ArrayList<byte[]> buckets = new ArrayList<byte[]>();


    public ByteList() {
        buckets.add(new byte[conf.bucketSize]);
    }

    public ByteList(Integer bucketSizeInBytes) {
        if (bucketSizeInBytes != null) {
            this.conf.bucketSize = bucketSizeInBytes;
        }
        buckets.add(new byte[conf.bucketSize]);
    }


    /**
     * Write data to storage, and return index where is written.
     * Index points to data header.
     */
    public long add(byte[] data) {
        final int objSize = conf.dataHeaderSize + data.length;
        final short dataLength = BitsAndBytes.toUnsignedShort(data.length);
        final long writePos = size.getAndAdd(objSize);
        final long endPos = writePos + objSize - 1;

        final byte[] startBucket = bucketForPosition(writePos);
        final int bucketPosition = (int) (writePos % conf.bucketSize);
        final int freeSpace = startBucket.length - bucketPosition;

        // most common case, data fits bucket, write header, data
        if (freeSpace >= objSize) {
            BitsAndBytes.writeShort(dataLength, startBucket, bucketPosition);
            System.arraycopy(data, 0, startBucket, bucketPosition + conf.dataHeaderSize, data.length);
            return writePos;
        }

        byte[] endBucket = bucketForPosition(endPos);

        // write header and upper bytes of in startBucket, rest of data to endBucket
        if (freeSpace > conf.dataHeaderSize) {
            BitsAndBytes.writeShort(dataLength, startBucket, bucketPosition);
            int bucketIdx = bucketPosition + conf.dataHeaderSize;
            int dataEnd = Math.min(data.length, startBucket.length - bucketIdx);
            System.arraycopy(data, 0, startBucket, bucketIdx, dataEnd);
            System.arraycopy(data, dataEnd, endBucket, 0, data.length - dataEnd);
            return writePos;
        }

        // write header to startBucket, data to endBucket
        if (freeSpace == 2) {
            BitsAndBytes.writeShort(dataLength, startBucket, bucketPosition);
            System.arraycopy(data, 0, endBucket, 0, data.length);
            return writePos;
        }

        // write first byte of header to startBucket, second byte of header, and all data to endBucket
        if (freeSpace == 1) {
            BitsAndBytes.splitShort(dataLength, startBucket, endBucket);
            System.arraycopy(data, 0, endBucket, 1, data.length);
            return writePos;
        }

        throw new IllegalStateException("If you can see this, it is a bug withing TimeCache implementation. freeSapce == " + freeSpace);
    }

    byte[] bucketForPosition(long objPos) {
        try {
            int bucketIndex = (int) (objPos / conf.bucketSize);
            byte[] bucket = buckets.get(bucketIndex);
            return bucket;
        }
        catch (IndexOutOfBoundsException exc) {
            synchronized (this) {
                int bucketIndex = (int) (objPos / conf.bucketSize);
                while (bucketIndex >= buckets.size())
                    buckets.add(new byte[conf.bucketSize]);
                byte[] bucket = buckets.get(bucketIndex);
                return bucket;
            }
        }
    }


    public <T> T peek(long objPos, Peeker<T> peeker) {
        final int objLength = objectLength(objPos);
        T res = readObject(objPos, objLength, peeker);
        return res;
    }


    public int objectLength(final long objPos) {
        final int bucketIdx = (int) (objPos / conf.bucketSize);
        if (bucketIdx >= buckets.size()) return -1;

        final byte[] startBucket = buckets.get(bucketIdx);
        final int objIdx = (int) (objPos % conf.bucketSize);
        final int freeSpace = startBucket.length - objIdx;

        if (freeSpace < conf.dataHeaderSize) {
            if (buckets.size() == bucketIdx + 1) return -1;
            short dataLen = BitsAndBytes.readSplitShort(startBucket, buckets.get(bucketIdx + 1));
            return dataLen;
        }

        short dataLen = BitsAndBytes.readShort(startBucket, objIdx);
        return dataLen;
    }


    <T> T readObject(final long objPos, final int dataLen, Peeker<T> userPeeker) {
        final long dataPos = objPos + conf.dataHeaderSize;
        final int bucketIdx = (int) (dataPos / conf.bucketSize);
        final byte[] startBucket = buckets.get(bucketIdx);
        final int dataIdx = (int) (dataPos % conf.bucketSize);
        final int endingPosition = dataIdx + dataLen;

        // data is fully contained within one bucket
        if (endingPosition <= startBucket.length) {
            return userPeeker.peek(objPos, startBucket, dataIdx, dataLen);
        }

        // data is split a between two buckets
        final byte[] data = new byte[conf.dataHeaderSize + dataLen];
        BitsAndBytes.writeShort((short) dataLen, data, 0);

        final int len1 = startBucket.length - dataIdx;
        System.arraycopy(startBucket, dataIdx, data, conf.dataHeaderSize, len1);

        final int len2 = dataLen - len1;
        final byte[] endBucket = buckets.get(bucketIdx + 1);
        System.arraycopy(endBucket, 0, data, len1 + conf.dataHeaderSize, len2);

        return userPeeker.peek(objPos, data, conf.dataHeaderSize, dataLen);
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

    public <T> DataIterator<T> iterator(Peeker<T> peeker) {
        return new DataIterator<>(peeker, 0, -1);
    }

    public void forEach(Peeker<?> userPeeker) {
        DataIterator<?> iterator = iterator(userPeeker);
        while (iterator.hasNext()) {
            iterator.next();
        }
    }



    @AllArgsConstructor
    public class DataIterator<T> implements Iterator<T> {
        Peeker<T> peeker;
        long objPostion = 0;
        int objLength = -1;

        public boolean hasNext() {
            objLength = objectLength(objPostion);
            return objLength > 0;
        }

        public T next() {
            if (objLength <= 0) throw new RuntimeException();
            T res = readObject(objPostion, objLength, peeker);
            objPostion += conf.dataHeaderSize + objLength;
            return res;
        }
    }



    public long getUsedSize() {
        return size.get();
    }

    public long getAllocatedSize() {
        return buckets.size() * (long) conf.bucketSize;
    }
}
