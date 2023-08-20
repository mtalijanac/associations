package mt.fireworks.timecache;

class BitsAndBytes {


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

    static void splitShort(short val, byte[] top, byte[] bottom) {
        top[top.length - 1] = (byte) (val >>> 8);
        bottom[0]           = (byte) val;
    }

    static short readSplitShort(byte[] top, byte[] bottom) {
        short res = (short) (	           // low two bytes of int composed
                (0xff & top[top.length - 1]) << 8    // from high byte
              | (0xff & bottom[0])     // and low byte
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


    public static int compare(
        byte[] a, int aFromIndex, int aToIndex,
        byte[] b, int bFromIndex, int bToIndex
    ) {
        int aLength = aToIndex - aFromIndex;
        int bLength = bToIndex - bFromIndex;
        int l = aLength - bLength;
        if (l != 0) return l;

        for (int idx = 0; idx < aLength; idx++) {
            byte aVal = a[aFromIndex + idx];
            byte bVal = b[bFromIndex + idx];
            int res = Byte.compare(aVal, bVal);
            if (res != 0) return res;
        }

        return 0;
    }

}
