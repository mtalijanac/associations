package mt.fireworks.timecache;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import mt.fireworks.timecache.TimeKeys;

public class TimeKeysTest {

    @Test
    public void usageExample() {
        long now = System.currentTimeMillis();
        long index = 0xCAFEBABEl;

        TimeKeys tk = new TimeKeys();
        long key = tk.key(now, index);

        long expectedTstamp = (now / 1000l) * 1000l;
        long tstamp = tk.tstamp(key);
        assertEquals(expectedTstamp, tstamp);

        long index2 = tk.index(key);
        assertEquals(index, index2);
    }
}
