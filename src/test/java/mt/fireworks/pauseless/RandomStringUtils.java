package mt.fireworks.pauseless;

import java.util.concurrent.ThreadLocalRandom;

public class RandomStringUtils {

    static String randomAlphanumeric(int len) {
        // num ASCII range: 48 - 57
        // A-Z ASCII range: 65 - 90
        // a-z ASCII range: 97 - 122

        char[] cs = new char[len];
        ThreadLocalRandom rng = ThreadLocalRandom.current();

        for (int i = 0; i < len; i++) {
            int rn = rng.nextInt(62);
            char ch = 0;
            if (rn < 10) {
                ch = (char) ((int) '0' + rn);
            }
            else if (10 <= rn && rn < 36) {
                ch = (char) ((int) 'A' + rn - 10);
            }
            else {
                ch = (char) ((int) 'a' + rn - 36);
            }

            cs[i] = ch;
        }

        String res = new String(cs);
        return res;
    }
}
