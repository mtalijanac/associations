package mt.fireworks.associations;

/** Utility manipulation of bits and bytes. */
public class BitsAndBytes {

    public static void writeInt(int val, byte[] arr, int idx) {
        arr[idx]     = (byte) (val >>> 24);
        arr[idx + 1] = (byte) (val >>> 16);
        arr[idx + 2] = (byte) (val >>> 8);
        arr[idx + 3] = (byte) val;
    }
    
    public static int readInt(byte[] arr, int idx) {
        int res = (int) (
                (0xff & arr[idx])     << 24
              | (0xff & arr[idx + 1]) << 16
              | (0xff & arr[idx + 2]) << 8
              | (0xff & arr[idx + 3])
        );
       return res;
    }

    public static void writeShort(short val, byte[] arr, int idx) {
        arr[idx]     = (byte) (val >>> 8);
        arr[idx + 1] = (byte) val;
    }

    public static short readShort(byte[] arr, int idx) {
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

    /** Write short to two arrays. High byte at end of {@code top} array, low byte at start of {@code bottom}. */
    public static void splitShort(short val, byte[] top, byte[] bottom) {
        top[top.length - 1] = (byte) (val >>> 8);
        bottom[0]           = (byte) val;
    }


    /** Read short written by {@code #splitShort(short, byte[], byte[])}. */
    public static short readSplitShort(byte[] top, byte[] bottom) {
        short res = (short) (	           // low two bytes of int composed
                (0xff & top[top.length - 1]) << 8    // from high byte
              | (0xff & bottom[0])     // and low byte
       );
       return res;
    }


    public static int readUnsignedShort(byte[] arr, int idx) {
        return 0xFFFF & readShort(arr, idx);
    }

    public static short toUnsignedShort(long val) {
        return (short) (0xFFFFl & val);
    }

    public static short toUnsignedShort(int val) {
        return (short) (0xFFFFl & val);
    }


    public static int imask(int bits) {
        if (bits == 32)
            return 0xFFFF_FFFF;
        return (1 << bits) - 1;
    }

    public static long lmask(int bits) {
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


    /** @return number of bytes needed to store value */
    public static int byteSize(long value) {
        if (value >= -0x80              && value <= 0x7F) return 1;
        if (value >= -0x8000            && value <= 0x7FFF) return 2;
        if (value >= -0x800000          && value <= 0x7FFFFF) return 3;
        if (value >= -0x80000000        && value <= 0x7FFFFFFF) return 4;
        if (value >= -0x8000000000L     && value <= 0x7FFFFFFFFFL) return 5;
        if (value >= -0x800000000000L   && value <= 0x7FFFFFFFFFFFL) return 6;
        if (value >= -0x80000000000000L && value <= 0x7FFFFFFFFFFFFFL) return 7;
        return 8; // Value can be stored in 8 bytes (long range)
    }



    public static String toHexString(byte[] arr) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (byte b: arr) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) sb.append("0");
            sb.append(hex).append(" ");
        }
        sb.setLength(sb.length() - 1);
        sb.append("]");
        return sb.toString();
    }
}

