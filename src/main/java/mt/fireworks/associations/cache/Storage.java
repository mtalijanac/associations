package mt.fireworks.associations.cache;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import lombok.*;
import mt.fireworks.associations.ByteList;
import mt.fireworks.associations.ByteList.DataIterator;
import mt.fireworks.associations.ByteList.Peeker;

class Storage {

    // config parameters for this storage
    final Conf conf;

    // stores key epoch, shared across cache
    final TimeKeys timeKeys;

    // cache time windows, each window store one timespan
    final ArrayList<Window> windows = new ArrayList<>();

    // window where events happening at currentTime would enter
    Window nowWindow;

    // windows access lock
    final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    @Getter
    final StorageMetric metric = new StorageMetric();


    Storage(TimeKeys timeKeys) {
        this(new Conf(), System.currentTimeMillis(), timeKeys);
    }


    Storage(Conf conf, Long startDate, TimeKeys timeKeys) {
        this.conf = conf == null ? new Conf() : conf;
        this.timeKeys = timeKeys;

        long start = startDate != null ? TimeKeys.normalizieTimestamp(startDate)
                                       : TimeKeys.normalizieTimestamp(System.currentTimeMillis());

        for (int idx = -1 * this.conf.historyWindowCount; idx <= this.conf.futureWindowCount; idx++) {
            Window win = new Window();
            win.startTstamp = start + idx * this.conf.windowTimespanMs;
            win.endTstamp = win.startTstamp + this.conf.windowTimespanMs;
            win.store = new ByteList(this.conf.allocationSize);
            this.windows.add(win);

            if (idx == 0) {
                this.nowWindow = win;
            }
        }
    }



    /** return unsafe index of window to which this tstamp belong */
    int windowIndexForTstamp(long tstamp) {
        for (int idx = windows.size() - 1; idx > -1; idx--) {
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

        metric.bytesWritten.addAndGet(data.length);
        long start = System.nanoTime();

        Window window = windowForTstamp(tstamp);
        if (window == null) return 0;
        long storeIndex = window.store.add(data);
        long key = timeKeys.key(tstamp, storeIndex);

        long end = System.nanoTime();
        metric.writeDuration.addAndGet(end - start);

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
     * @see CacheSerDes#unmarshall(byte[], int, int)
     */
    public <T> T getEntry2(long key, CacheSerDes<T> serdes) {
        long tstamp = timeKeys.tstamp(key);
        long index = timeKeys.index(key);
        Window window = windowForTstamp(tstamp);
        if (window == null) return null;
        T val = window.store.peek(index, (objPos, bucket, pos, len) -> serdes.unmarshall(bucket, pos, len));
        return val;
    }

    /** @return true if value under key equal to passed data? */
    public boolean equal(long key, byte[] data, CacheSerDes<?> serdes) {
        long tstamp = timeKeys.tstamp(key);
        long index = timeKeys.index(key);
        Window window = windowForTstamp(tstamp);
        if (window == null) return false;
        Boolean res = window.store.peek(index, (objPos, bucket, pos, len) -> {
            return serdes.equalsD(bucket, pos, len, data, 0, data.length);
        });
        return res;
    }


    /** @return cache time range */
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
        win.store = new ByteList(conf.allocationSize);
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


    public <T> Iterator<T> iterator(Peeker<T> peeker) {
        return new StorageIterator<>(windows, peeker);
    }

    public static class StorageIterator<T> implements Iterator<T> {
        Peeker<T> peeker;
        Iterator<DataIterator<T>> higherIterator;
        DataIterator<T> lowerIterator;

        public StorageIterator(List<Window> windows, Peeker<T> peeker) {
            ArrayList<Window> winCopy = new ArrayList<>(windows);
            ArrayList<DataIterator<T>> dataIterators = new ArrayList<>();
            for (Window win: winCopy) {
                DataIterator<T> dataIterator = win.store.iterator(peeker);;
                dataIterators.add(dataIterator);
            }
            higherIterator = dataIterators.iterator();
        }

        public boolean hasNext() {
            if (lowerIterator == null) {
                if (!higherIterator.hasNext()) {
                    return false;
                }
                lowerIterator = higherIterator.next();
            }

            if (lowerIterator.hasNext()) {
                return true;
            }

            while (higherIterator.hasNext()) {
                lowerIterator = higherIterator.next();
                if (lowerIterator.hasNext()) {
                    return true;
                }
            }

            return false;
        }

        public T next() {
            return lowerIterator.next();
        }
    }



    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Storage summary\n");
        sb.append("Window count: ").append(windows.size()).append("\n");
        for (int idx = 0; idx < windows.size(); idx++) {
            Window win = windows.get(idx);
            sb.append("  ").append(idx + 1).append(". ");
            sb.append(win.startTstamp).append(" - ").append(win.endTstamp);
            sb.append(" [").append(TimeUtils.readableTstamp(win.startTstamp)).append(" - ");
            sb.append(TimeUtils.readableTstamp(win.endTstamp)).append("]");
            if (win.closed.get()) sb.append(" CLOSED");
            sb.append("\n");
        }
        return sb.toString();
    }


    @Data
    class StorageMetric implements Metrics {
        String name = "StorageMetric";

        AtomicLong bytesWritten = new AtomicLong();
        AtomicLong writeDuration = new AtomicLong();

        @Override
        public String text(boolean coments) {
            String durStr = TimeUtils.toReadable(writeDuration.get());
            double speed = 1_000_000_000d * bytesWritten.get() / writeDuration.get() / 1024d / 1024d;
            String speedStr = String.format("%.2f Mb/sec", speed);

            long[] timespan = Storage.this.timespan();
            String from = TimeUtils.readableTstamp(timespan[0]);
            String to = TimeUtils.readableTstamp(timespan[1]);

            String windows = "[" + Storage.this.conf.historyWindowCount + "-1-" + Storage.this.conf.futureWindowCount + "]";
            String winSize =  Storage.this.conf.windowTimespanMs + " ms (" + TimeUtils.toReadable(Storage.this.conf.windowTimespanMs * 1000_000) + ")";

            ArrayList<Window> wins = Storage.this.windows;
            long totalAllocated = 0;
            long totalUsed = 0;
            for (Window w: wins) {
                ByteList store = w.store;
                totalAllocated += store.getAllocatedSize();
                totalUsed += store.getUsedSize();
            }

            String text = "## " + name + " metric:\n"
                        + " bytesWritten: " + bytesWritten + " bytes, dur: " + durStr + " [" + speedStr + "]\n"
                        + " window count: " + Storage.this.windows.size() + " " + windows + "\n"
                        + "  window span: " + winSize + "\n"
                        + "timespan from: " + from + "\n"
                        + "           to: " + to + "\n"
                        + "    allocated: " + totalAllocated + " bytes\n"
                        + "         used: " + totalUsed + " bytes\n"
                        + " win capacity: " + conf.allocationSize + " bytes";

            return text;
        }

        @Override
        public String reset() {
            String text = text(false);
            bytesWritten.set(0);
            writeDuration.set(0);
            return text;
        }
    }


    @Setter
    static class Conf {
        /** duration of window in ms */
        long windowTimespanMs = TimeUnit.DAYS.toMillis(1);

        /** count of windows preceding nowWindow */
        int historyWindowCount = 7;

        /** count of window following nowWindow */
        int futureWindowCount = 1;

        int allocationSize = 1 * 1024 * 1024;
    }

    static class Window {

        /** inclusive tstamp of oldest data in window */
        long startTstamp;

        /** exclusive tstamp of newest data in window */
        long endTstamp;

        /** if closed do not write to it */
        final AtomicBoolean closed = new AtomicBoolean(false);

        ByteList store;
    }
}
