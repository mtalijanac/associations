package mt.fireworks.timecache;

public class BitsAndBytes {

    static void writeShort(short val, byte[] arr, int idx) {
        arr[idx]     = (byte) (val >>> 8);
        arr[idx + 1] = (byte) val;
    }

    static short readShort(byte[] arr, int idx) {
        /* same code as:
            byte hb = src[pos];
            int hi = (0xff & hb) << 8;

            byte lb = src[pos + 1];
            int li = 0xff & lb;

            int val = hi | li;
            short res = (short) (0xffff & val);
        //*/

        short res = (short) (	           // low two bytes of int composed
                 (0xff & arr[idx]) << 8    // from high byte
               | (0xff & arr[idx + 1])     // and low byte
        );
        return res;
    }

    static int readUnsignedShort(byte[] arr, int idx) {
        return 0xFFFF & readShort(arr, idx);
    }

    static short toUnsignedShort(long val) {
        return (short) (0xFFFFl & val);
    }

    static short toUnsignedShort(int val) {
        return (short) (0xFFFFl & val);
    }

    static long byteLenToWordLen(long byteOffset, int wordSize) {
        if (wordSize == 0) return byteOffset;
        long words = byteOffset / wordSize;
        return words;
    }

    static long wordLenToByteLen(long words, int wordSize) {
        if (wordSize == 0) return words;
        return words * wordSize;
    }


    static int imask(int bits) {
        if (bits == 32)
            return 0xFFFF_FFFF;
        return (1 << bits) - 1;
    }

    static long lmask(int bits) {
        if (bits == 64)
            return 0xFFFF_FFFF_FFFF_FFFFl;
        return (1l << bits) - 1l;
    }

}
