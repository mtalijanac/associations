package mt.fireworks.timecache;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class ByteList {

    static class Conf {
        /** bucket size in bytes */
        int bucketSize = 1 * 1024 * 1024;

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

    byte[] bucketForPosition(long writePos) {
        try {
            int bucketIndex = (int) (writePos / conf.bucketSize);
            byte[] bucket = buckets.get(bucketIndex);
            return bucket;
        }
        catch (IndexOutOfBoundsException exc) {
            synchronized (this) {
                int bucketIndex = (int) (writePos / conf.bucketSize);
                while (bucketIndex >= buckets.size())
                    buckets.add(new byte[conf.bucketSize]);
                byte[] bucket = buckets.get(bucketIndex);
                return bucket;
            }
        }
    }



    <T> T peek(long key, Peeker<T> peeker) {
        int bucketIndex = (int) (key / conf.bucketSize);
        byte[] startBucket = buckets.get(bucketIndex);

        int objIdx = (int) (key % conf.bucketSize);
        int freeSpace = startBucket.length - objIdx;

        if (freeSpace == 1) {
            byte[] endBucket = buckets.get(bucketIndex + 1);
            short dataLen = BitsAndBytes.readSplitShort(startBucket, endBucket);
            T res = peeker.peek(key, endBucket, 1, dataLen);
            return res;
        }

        if (freeSpace == 2) {
            byte[] endBucket = buckets.get(bucketIndex + 1);
            int dataLen = BitsAndBytes.readUnsignedShort(startBucket, objIdx);
            T res = peeker.peek(key, endBucket, 0, dataLen);
            return res;
        }

        int dataLen = BitsAndBytes.readUnsignedShort(startBucket, objIdx);

        if (objIdx + conf.dataHeaderSize + dataLen > startBucket.length) {
            byte[] data = new byte[dataLen + 2];
            BitsAndBytes.writeShort((short) dataLen, data, 0);

            int toReadFromStartBucket = Math.min(startBucket.length - objIdx - 2, dataLen);
            System.arraycopy(startBucket, objIdx + conf.dataHeaderSize, data, 2, toReadFromStartBucket);
            byte[] endBucket = buckets.get(bucketIndex + 1);
            int toReadFromEndBucket = dataLen - toReadFromStartBucket;
            System.arraycopy(endBucket, 0, data, 2 + toReadFromStartBucket, toReadFromEndBucket);

            T res = peeker.peek(key, data, 2, dataLen);
            return res;
        }

        T res = peeker.peek(key, startBucket, objIdx + conf.dataHeaderSize, dataLen);
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
        long objPos = 0;
        while (true) {
            final int bucketIdx = (int) (objPos / conf.bucketSize);
            if (bucketIdx >= buckets.size()) break;

            final byte[] startBucket = buckets.get(bucketIdx);
            final int objIdx = (int) (objPos % conf.bucketSize);
            final int freeSpace = startBucket.length - objIdx;

            // split header
            if (freeSpace == 1) {
                if (buckets.size() == bucketIdx + 1) return;
                short dataLen = BitsAndBytes.readSplitShort(startBucket, buckets.get(bucketIdx + 1));
                if (dataLen == 0) return;
                byte[] endBucket = buckets.get(bucketIdx + 1);

                ForEachAction res = userPeeker.peek(objPos, endBucket, 1, dataLen);
                if (ForEachAction.BREAK.equals(res)) break;
                objPos += conf.dataHeaderSize + dataLen;
                continue;
            }

            // header here, data in next bucket
            if (freeSpace == 2) {
                if (buckets.size() == bucketIdx + 1) return;
                short dataLen = BitsAndBytes.readShort(startBucket, objIdx);
                if (dataLen == 0) return;
                byte[] endBucket = buckets.get(bucketIdx + 1);

                ForEachAction res = userPeeker.peek(objPos, endBucket, 0, dataLen);
                if (ForEachAction.BREAK.equals(res)) break;
                objPos += conf.dataHeaderSize + dataLen;
                continue;
            }


            short dataLen = BitsAndBytes.readShort(startBucket, objIdx);
            if (dataLen == 0)
                break;

            if (objIdx + conf.dataHeaderSize + dataLen > startBucket.length) {

                byte[] data = new byte[dataLen + 2];
                BitsAndBytes.writeShort((short) dataLen, data, 0);

                int toReadFromStartBucket = Math.min(startBucket.length - objIdx - 2, dataLen);
                System.arraycopy(startBucket, objIdx + conf.dataHeaderSize, data, 2, toReadFromStartBucket);
                byte[] endBucket = buckets.get(bucketIdx + 1);
                int toReadFromEndBucket = dataLen - toReadFromStartBucket;
                System.arraycopy(endBucket, 0, data, 2 + toReadFromStartBucket, toReadFromEndBucket);

                ForEachAction res = userPeeker.peek(objPos, data, 2, dataLen);
                objPos += conf.dataHeaderSize + dataLen;
                if (ForEachAction.BREAK.equals(res)) break;
                continue;
            }

            ForEachAction res = userPeeker.peek(objPos, startBucket, objIdx + conf.dataHeaderSize, dataLen);
            if (ForEachAction.BREAK.equals(res)) break;
            objPos += conf.dataHeaderSize + dataLen;
        }
    }

}
