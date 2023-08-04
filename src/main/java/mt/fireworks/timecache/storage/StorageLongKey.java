package mt.fireworks.timecache.storage;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import mt.fireworks.timecache.SerDes;
import mt.fireworks.timecache.storage.ByteList.Peeker;

public class StorageLongKey {

    @AllArgsConstructor
    @RequiredArgsConstructor
    static class Conf {
        /** count of windows preceeding nowWindow */
        int historyWindowCount = 7;

        /** count of window following nowWindow */
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


    final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    Conf conf = new Conf();
    ArrayList<Window> windows = new ArrayList<>();
    Window nowWindow;	// window where events happening at currentTime would enter
    public TimeKeys timeKeys;

    static StorageLongKey init() {
        return init(null, null, new TimeKeys());
    }

    static StorageLongKey init(Conf conf, Long start, TimeKeys timeKeys) {
        StorageLongKey st = new StorageLongKey();
        st.timeKeys = timeKeys;

        if (conf != null) {
            st.conf = conf;
        }

        long startDate = (System.currentTimeMillis() / 1000l) * 1000l;
        if (start != null) {
            startDate = (start / 1000l) * 1000l;
        }

        for (int idx = -1 * st.conf.historyWindowCount; idx <= st.conf.futureWindowCount; idx++) {
            Window win = new Window();
            win.startTstamp = startDate + idx * st.conf.windowTimespanMs;
            win.endTstamp = win.startTstamp + st.conf.windowTimespanMs;
            st.windows.add(win);

            if (idx == 0) {
                st.nowWindow = win;
            }
        }

        return st;
    }


    /** return unsafe index of window to which this tstamp belong */
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

    /** @return threadsafe dohvat prozora po timestmapu */
    Window windowForTstamp(long tstamp) {
        @Cleanup("unlock") ReadLock rock = rwLock.readLock();
        rock.lock();

        int winIndex = windowIndexForTstamp(tstamp);
        if (winIndex < 0) return null;
        Window window = windows.get(winIndex);
        return window;
    }


    /** @return address of stored data, or 0 if data is not storable */
    public long addEntry(long tstamp, byte[] data) {
        // add entry to a window bucket
        // generate key and return it

        Window window = windowForTstamp(tstamp);
        if (window == null) return 0;
        long storeIndex = window.store.add(data);
        long key = timeKeys.key(tstamp, storeIndex);
        return key;
    }


    /**
     * Fetch data stored under key. Returned array is newly allocated.
     * @return byte array of entry under key or null.
     **/
    public byte[] getEntry(long key) {
        long tstamp = timeKeys.tstamp(key);
        long index = timeKeys.index(key);
        Window window = windowForTstamp(tstamp);
        if (window == null) return null;
        byte[] data = window.store.get(index);
        return data;
    }


    /**
     * Read data stored under key, and unmarshall it using provided serdes.
     * Reading data will not allocate new byte array.
     * @return unmarshalled object or null if data not present.
     * @see SerDes#unmarshall(byte[], int, int)
     */
    public <T> T getEntry2(long key, SerDes<T> serdes) {
        long tstamp = timeKeys.tstamp(key);
        long index = timeKeys.index(key);
        Window window = windowForTstamp(tstamp);
        if (window == null) return null;
        T val = window.store.peek(index, (objPos, bucket, pos, len) -> serdes.unmarshall(bucket, pos, len));
        return val;
    }

    /** @return true if value under key equal to passed data? */
    public boolean equal(long key, byte[] data, SerDes serdes) {
        long tstamp = timeKeys.tstamp(key);
        long index = timeKeys.index(key);
        Window window = windowForTstamp(tstamp);
        if (window == null) return false;
        Boolean res = window.store.peek(index, (objPos, bucket, pos, len) -> {
            return serdes.equalsD(bucket, pos, len, data, 0, data.length);
        });
        return res;
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


    public Window moveWindows() {
        @Cleanup("unlock") WriteLock wock = rwLock.writeLock();
        wock.lock();

        // add future window
        int lastWinIndex = windows.size() - 1;
        Window lastWindow = windows.get(lastWinIndex);
        Window win = new Window();
        win.startTstamp = lastWindow.endTstamp;
        win.endTstamp = win.startTstamp + conf.windowTimespanMs;
        windows.add(win);

        // move now window
        int nowWinIdx = windowIndexForTstamp(nowWindow.endTstamp);
        Window newNowWindow = windows.get(nowWinIdx);
        nowWindow = newNowWindow;

        // remove oldest window
        Window oldestWin = windows.get(0);
        oldestWin.closed.set(true);
        windows.remove(0);

        return oldestWin;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Storage summary\n");
        sb.append("Window count: ").append(windows.size()).append("\n");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        for (int idx = 0; idx < windows.size(); idx++) {
            Window win = windows.get(idx);
            sb.append("  ").append(idx + 1).append(". ");
            sb.append(win.startTstamp).append(" - ").append(win.endTstamp);
            sb.append(" [").append(sdf.format(win.startTstamp)).append(" - ");
            sb.append(sdf.format(win.endTstamp)).append("]");
            if (win.closed.get()) sb.append(" CLOSED");
            sb.append("\n");
        }
        return sb.toString();
    }
}
