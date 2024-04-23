package mt.fireworks.pauseless;

public class BitsAndBytes {

    public static long bytes2long(byte[] bytes, int offset, int len) {
        if (len > 8)
            throw new IllegalArgumentException("len must be <= 8 and now is: " + len);

        if (offset >= bytes.length)
            throw new IllegalArgumentException("offset beyond byte array length");

        final int endIdx = Math.min(offset + len, bytes.length);
        long val = 0;
        for (int idx = offset; idx < endIdx; idx++) {
            byte b = bytes[idx];
            val = (val << 8) | (0xFFl & b);
        }
        return val;
    }

}
