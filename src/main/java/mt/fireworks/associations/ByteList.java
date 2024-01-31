package mt.fireworks.associations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import lombok.AllArgsConstructor;

/**
 * Same as byteList but with variable header size.
 */
public class ByteList {

    @FunctionalInterface
    public interface Peeker<T> {
        T peek(long objPos, byte[] bucket, int pos, int len);
    }

    final ArrayList<byte[]> buckets = new ArrayList<>();
    final AtomicLong size = new AtomicLong();
    final int bucketSize;

    public ByteList(int allocationSize) {
        this.bucketSize = allocationSize;
    }

    public ByteList() {
        this(1 * 1024 * 1024);
    }


    public long add(byte[] data) {
        if (data.length > bucketSize) {
            String msg = "Illegal data size. "
                       + "Adding data of size: " + data.length + " bytes, "
                       + "while allocationSize is set to: " + bucketSize + " bytes. "
                       + "To add data this big increase allocation size.";
            throw new RuntimeException(msg);
        }

        final int headerSize = ObjHeader.headerSize(data.length);
        final int objectSize = headerSize + data.length;
        final long objStartPos = newObject(objectSize);

        final byte[] startBuck = bucketForPosition(objStartPos);
        final byte[] endBuck = bucketForPosition(objStartPos + objectSize);

        final int headerIdx = (int) (objStartPos % bucketSize);
        ObjHeader.writeHeader(data.length, startBuck, endBuck, headerIdx);

        final int dataOffset = (int)((objStartPos + headerSize) % bucketSize);

        // everything fits in top bucket
        if (startBuck == endBuck) {
            System.arraycopy(data, 0, startBuck, dataOffset, data.length);
            return objStartPos;
        }

        final byte[] dataStartBucket = bucketForPosition(objStartPos + headerSize);

        // split header, data is fully in bottom bucket
        if (dataStartBucket == endBuck) {
            System.arraycopy(data, 0, endBuck, dataOffset, data.length);
            return objStartPos;
        }

        // split data, data goes to top, then it pours to bottom
        int len = bucketSize - dataOffset;
        System.arraycopy(data, 0, startBuck, dataOffset, len);
        System.arraycopy(data, len, endBuck, 0, data.length - len);
        return objStartPos;
    }


    byte[] bucketForPosition(long objPos) {
        int bucketIndex = (int) (objPos / bucketSize);
        byte[] bucket = buckets.get(bucketIndex);
        return bucket;
    }


    long newObject(int objectSize) {
        long objPos = size.getAndAdd(objectSize);
        allocateBucketForPosition(objPos + objectSize);
        return objPos;
    }

    synchronized
    void allocateBucketForPosition(long objPos) {
        int bucketIndex = (int) (objPos / bucketSize);
        while (bucketIndex >= buckets.size())
            buckets.add(new byte[bucketSize]);
    }



    public <T> T peek(long objPos, Peeker<T> peeker) {
        final int dataLength = dataLength(objPos);
        final int headerSize = ObjHeader.headerSize(dataLength);

        final long dataPosition = objPos + headerSize;
        final long dataEndPosition = dataPosition + dataLength;

        final byte[] startDataBucket = bucketForPosition(dataPosition);
        final byte[] endDataBucket = bucketForPosition(dataEndPosition);

        if (startDataBucket == endDataBucket) {
            int offset = (int) (dataPosition % bucketSize);
            T val = peeker.peek(objPos, startDataBucket, offset, dataLength);
            return val;
        }


        // DATA is split between two buckets
        int objectSize = headerSize + dataLength;
        byte[] objCpy = new byte[objectSize];
        ObjHeader.writeHeader(dataLength, objCpy, null, 0);
        int offset = (int) (dataPosition % bucketSize);
        int startReadLen = bucketSize - offset;
        int endReadLen = dataLength - startReadLen;
        System.arraycopy(startDataBucket, offset, objCpy, headerSize, startReadLen);
        System.arraycopy(endDataBucket, 0, objCpy, headerSize + startReadLen, endReadLen);
        T val = peeker.peek(objPos, objCpy, headerSize, dataLength);
        return val;
    }


    /** @return Length of data stored under key. */
    public int dataLength(long objPos) {
        byte[] startBuck = bucketForPosition(objPos);
        int bucketOffset = (int) (objPos % bucketSize);
        int headerSize = ObjHeader.headerSize(startBuck, bucketOffset);
        byte[] headerEndBuck = bucketForPosition(objPos + headerSize); // FIXME beskoristan pozivi, može se izračunati iz offseta + len
        int dataLength = ObjHeader.readHeader(startBuck, headerEndBuck, bucketOffset);
        return dataLength;
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

    public long getUsedSize() {
        return size.get();
    }

    public long getAllocatedSize() {
        return buckets.size() * (long) bucketSize;
    }



    public <T> DataIterator<T> iterator(Peeker<T> peeker) {
        return new DataIterator<>(peeker, 0);
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
        long objPos = 0;

        public boolean hasNext() {
            if (ByteList.this.buckets.size() == 0) {
                return false;
            }

            final int dataLength = dataLength(objPos);
            return dataLength > 0;
        }

        public T next() {
            T res = peek(objPos, peeker);
            int len = dataLength(objPos);
            int headerSize = ObjHeader.headerSize(len);
            objPos += headerSize + len;
            return res;
        }
    }




    @Deprecated
    public int length(long objPos) {
        return dataLength(objPos);
    }

    /**
     * Object header is var length number which encodes length of data following it.
     * First 2 bits of header are length of header:
     * <ul>
     * <li>00 - 1 bytes</li>
     * <li>01 - 2 bytes</li>
     * <li>10 - 3 bytes</li>
     * <li>11 - 4 bytes</li>
     * </ul>
     *
     * Bits & bytes following is unsigned value stored in header. */
    static class ObjHeader {

        static int headerSize(byte[] arr, int off) {
            byte b = arr[off];
            int headerLen = ((0xC0 & b) >>> 6) + 1;
            return headerLen;
        }


        /* @return for given value return byte length of header */
        static int headerSize(int value) {
            if (value < 0x3F) return 1;
            if (value <= 0x3FFF) return 2;
            if (value <= 0x3FFFFF) return 3;
            return 4;
        }

        /** @return value encoded as header */
        static int header(int value) {
            if (value <= 0x3F) return value;                       // 63 bytes data, 1 byte header
            if (value <= 0x3FFF) return 0x4000 | (0x3FFF & value); // 16 kb, 2 byte header
            if (value <= 0x3FFFFF) return 0x80_0000 | (0x3F_FFFF & value); // 4mb, 3 byte header
            return 0xC000_0000 | (0x3FFF_FFFF & value);            // 1gb, 4 byte header
        }

        static void writeHeader(int val, byte[] top, byte[] bottom, int pos) {
            int bytesSize = headerSize(val);
            int header = header(val);
            for (int off = 0; off < bytesSize; off++) {
                byte b = (byte)(header >>> (8 * (bytesSize - 1 - off)));
                int idx = pos + off;
                byte[] des = top;
                if (idx >= top.length) {
                    idx = idx - top.length;
                    des = bottom;
                }
                des[idx] = b;
            }
        }

        static int readHeader(byte[] top, byte[] bottom, int pos) {
            byte b = top[pos];
            int val = 0x3F & b;
            int headerLen = ((0xC0 & b) >>> 6) + 1;

            for (int off = 1; off < headerLen; off++) {
                byte[] src = top;
                int idx = pos + off;
                if (idx >= top.length) {
                    idx = idx - top.length;
                    src = bottom;
                }
                b = src[idx];

                int shiftedVal = val << 8;
                int maskedB = b & 0xFF;
                val = shiftedVal | maskedB;
            }
            return val;
        }
    }
}
