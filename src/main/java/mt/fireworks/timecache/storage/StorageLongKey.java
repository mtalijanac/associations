package mt.fireworks.timecache.storage;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import mt.fireworks.timecache.SerDes2;

public class StorageLongKey {

    static class Conf {
        /** total count of windows */
        int historyWindowCount = 7;
        int futureWindowCount = 1;

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
    Window nowWindow;	// window where events happening at currentTime would enter
    public TimeKeys timeKeys = new TimeKeys();

    public static StorageLongKey init() {
        StorageLongKey st = new StorageLongKey();
        final long startDate = (System.currentTimeMillis() / 1000l) * 1000l;

        for (int idx = -1 * st.conf.historyWindowCount; idx <= st.conf.futureWindowCount; idx++) {
            Window win = new Window();
            win.startTstamp = startDate - idx * st.conf.windowTimespanMs;
            win.endTstamp = win.startTstamp + st.conf.windowTimespanMs;
            st.windows.add(win);

            if (idx == 0) {
                st.nowWindow = win;
            }
        }

        return st;
    }


    /** @return address of stored data, or 0 if data is not storable */
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
        if (winIndex < 0) return 0;
        Window window = windows.get(winIndex);

        long storeIndex = window.store.add(data);
        long key = timeKeys.key(tstamp, storeIndex);
        return key;
    }


    /* return index of window to which this tstamp belong */
    int windowIndexForTstamp(long tstamp) {
        for (int idx = 0; idx < windows.size(); idx++) {
            Window win = windows.get(idx);
            if (win.closed.get()) {
                continue;
            }

            if (win.startTstamp <= tstamp && tstamp < win.endTstamp) {
                return idx;
            }
        }

        return -1;
    }

    public byte[] getEntry(long key) {
        long tstamp = timeKeys.tstamp(key);
        long index = timeKeys.index(key);

        int winIndex = windowIndexForTstamp(tstamp);
        if (winIndex < 0) return null;
        Window window = windows.get(winIndex);
        byte[] data = window.store.get(index);
        return data;
    }

    public <T> T getEntry2(long key, SerDes2<T> serdes) {
        long tstamp = timeKeys.tstamp(key);
        long index = timeKeys.index(key);

        int winIndex = windowIndexForTstamp(tstamp);
        if (winIndex < 0) return null;
        Window window = windows.get(winIndex);
        T val = window.store.peek(index, (bucket, pos, len) -> serdes.unmarshall(bucket, pos, len));
        return val;
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
