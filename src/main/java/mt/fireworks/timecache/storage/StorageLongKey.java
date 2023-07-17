package mt.fireworks.timecache.storage;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class StorageLongKey {

    static class Conf {
        /** num. of leading bits of key encoding window */
        int winIndexBits = 3;

        /** total count of windows */
        int windowCount = 8;

        /** duration of window in ms */
        long windowTimespanMs = TimeUnit.DAYS.toMillis(1);
    }

    static class Window {

        /** inclusive tstamp of oldest data in window */
        long startTstamp;

        /** exclusive tstamp of newest data in window */
        long endTstamp;

        /** if closed do not write to it */
        final AtomicBoolean closed = new AtomicBoolean(false);

        ByteList store = new ByteList();
    }


    Conf conf = new Conf();
    ArrayList<Window> windows = new ArrayList<>();
    public TimeKeys timeKeys = new TimeKeys();


    public static StorageLongKey init() {
        StorageLongKey st = new StorageLongKey();
        final int winCount = 1 << st.conf.winIndexBits;
        final long startDate = (System.currentTimeMillis() / 1000l) * 1000l;

        for (int idx = 0; idx < winCount; idx++) {
            Window win = new Window();
            win.startTstamp = startDate - idx * st.conf.windowTimespanMs;
            win.endTstamp = win.startTstamp + st.conf.windowTimespanMs;
            st.windows.add(win);
        }

        return st;
    }


    public long addEntry(long tstamp, byte[] data) {
        // find window based on tstamp
            // TODO
            // if tstamp is older than oldest window
            // either ignore or store it to oldest window

            // TODO
            // if tstamp is younger than end of youngest window
            // either ignore or store it to youngest window

        // add entry to a window bucket
        // generate key and return it

        int winIndex = windowIndexForTstamp(tstamp);
        Window window = windows.get(winIndex);

        long storeIndex = window.store.add(data);
        long key = timeKeys.key(tstamp, storeIndex);
        return key;
    }


    /* return index of window to which this tstamp belong */
    int windowIndexForTstamp(long tstamp) {
        long minTstamp = Long.MAX_VALUE;
        int minIndex = -1;

        long maxTstamp = Long.MIN_VALUE;
        int maxIndex = -1;

        for (int idx = 0; idx < windows.size(); idx++) {
            Window win = windows.get(idx);
            if (win.closed.get()) {
                continue;
            }

            if (win.startTstamp <= tstamp && tstamp < win.endTstamp) {
                return idx;
            }

            if (win.startTstamp < minTstamp) {
                minTstamp = win.startTstamp;
                minIndex = idx;
            }

            if (win.endTstamp > maxTstamp) {
                maxTstamp = win.endTstamp;
                maxIndex = idx;
            }
        }

        return tstamp < minTstamp ? minIndex : maxIndex;
    }

    public byte[] getEntry(long key) {
        long tstamp = timeKeys.tstamp(key);
        long index = timeKeys.index(key);

        int winIndex = windowIndexForTstamp(tstamp);
        Window window = windows.get(winIndex);
        byte[] data = window.store.get(index);
        return data;
    }


    /** return cachable time range */
    public long[] timespan() {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (Window w: windows) {
            min = Math.min(min, w.startTstamp);
            max = Math.max(max, w.endTstamp);
        }
        return new long[] {min, max};
    }


    public void moveWindows() {
        // allocate new one
        // find oldest window
        // mark it closed

    }
}
