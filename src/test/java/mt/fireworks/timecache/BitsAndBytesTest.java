package mt.fireworks.timecache;

import static mt.fireworks.timecache.BitsAndBytes.*;
import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import mt.fireworks.timecache.BitsAndBytes;

public class BitsAndBytesTest {

    @Test
    public void writeShortTest() {
        byte[] dest = new byte[2];

        for (short v = Short.MIN_VALUE; v <= Short.MAX_VALUE; v++) {
            writeShort(v, dest, 0);
            int res = readShort(dest, 0);
            assertEquals(v, res);

            if (v == Short.MAX_VALUE) break;
        }
    }

    @Test
    public void writeShortRandomPlacementTest() {
        byte[] dest = new byte[100 + 2];
        Random rng = new Random();

        for (short v = Short.MIN_VALUE; v <= Short.MAX_VALUE; v++) {
            int pos = rng.nextInt(100);
            writeShort(v, dest, pos);
            int res = readShort(dest, pos);
            assertEquals(v, res);

            if (v == Short.MAX_VALUE) break;
        }
    }

    @Test
    public void imask() {
        Object[] val = {
             1, "1",
             2, "11",
             3, "111",
             4, "1111",
             5, "11111",
             6, "111111",
             7, "1111111",
             8, "11111111",
             9, "111111111",
            10, "1111111111",
            11, "11111111111",
            12, "111111111111",
            13, "1111111111111",
            14, "11111111111111",
            15, "111111111111111",
            16, "1111111111111111",
            17, "11111111111111111",
            18, "111111111111111111",
            19, "1111111111111111111",
            20, "11111111111111111111",
            21, "111111111111111111111",
            22, "1111111111111111111111",
            23, "11111111111111111111111",
            24, "111111111111111111111111",
            25, "1111111111111111111111111",
            26, "11111111111111111111111111",
            27, "111111111111111111111111111",
            28, "1111111111111111111111111111",
            29, "11111111111111111111111111111",
            30, "111111111111111111111111111111",
            31, "1111111111111111111111111111111",
            32, "11111111111111111111111111111111"
        };
        for (int idx = 0; idx < val.length; idx += 2) {
            int bits = (Integer) val[idx];
            String expectedMask = (String) val[idx + 1];
            int mask = BitsAndBytes.imask(bits);
            String maskStr = Integer.toBinaryString(mask);
            Assert.assertEquals(expectedMask, maskStr);
        }
    }

}
