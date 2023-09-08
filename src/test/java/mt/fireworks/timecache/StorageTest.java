package mt.fireworks.timecache;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import lombok.AllArgsConstructor;
import lombok.Data;
import mt.fireworks.timecache.ByteList.Peeker;


public class StorageTest {

    byte[] randomData(int min, int max) {
        int len = ThreadLocalRandom.current().nextInt(min, max);
        byte[] data = new byte[len];
        ThreadLocalRandom.current().nextBytes(data);
        return data;
    }

    @Test
    public void debug() {
        Storage storage = Storage.init();

        byte[] someData_1 = randomData(250, 500);
        ByteBuffer.wrap(someData_1).putInt(0x5ca1ab1e);
        long tstamp_1 = System.currentTimeMillis();

        long key1 = storage.addEntry(tstamp_1, someData_1);
        long index = storage.timeKeys.index(key1);

        byte[] readData_1 = storage.getEntry(key1);
        Assert.assertArrayEquals(someData_1, readData_1);
    }


    @Test
    public void usageExample() {
        // prepare some data
        byte[] someData_1 = randomData(250, 500);
        ByteBuffer.wrap(someData_1).putInt(0x5ca1ab1e);
        long tstamp_1 = System.currentTimeMillis() + 100;

        byte[] someData_2 = randomData(250, 500);
        ByteBuffer.wrap(someData_2).putInt(0xCAFEBABE);
        long tstamp_2 = System.currentTimeMillis() + 10000;


        // init storage
        Storage storage = Storage.init();


        // write data
        long key1 = storage.addEntry(tstamp_1, someData_1);
        long key2 = storage.addEntry(tstamp_2, someData_2);


        // read data:
        byte[] readData_1 = storage.getEntry(key1);
        Assert.assertArrayEquals(someData_1, readData_1);

        byte[] readData_2 = storage.getEntry(key2);
        Assert.assertArrayEquals(someData_2, readData_2);
    }

    @Test
    public void testWithLotOfData() {
        Storage storage = Storage.init();
        long[] timespan = storage.timespan();

        for (int i = 0; i < 2_000_000; i++) {
            long tstamp = ThreadLocalRandom.current().nextLong(timespan[0], timespan[1]);
            byte[] data = randomData(250, 1500);
            long key = storage.addEntry(tstamp, data);
            byte[] dataRead = storage.getEntry(key);
            assertArrayEquals(data, dataRead);
        }
    }

    @Test
    public void testWithLotOfData2() {
        Storage storage = Storage.init();
        long[] timespan = storage.timespan();

        long[] counter = new long[8];

        for (int i = 0; i < 4_000_000; i++) {
            long tstamp = System.currentTimeMillis();
            byte[] data = randomData(250, 320);
            long key = storage.addEntry(tstamp, data);
            byte[] dataRead = storage.getEntry(key);
            assertArrayEquals(data, dataRead);

            int winIdx = storage.windowIndexForTstamp(tstamp);
            counter[winIdx]++;
        }
    }

    @Test
    public void windowAllocationTest() {
        Storage storage = Storage.init();
        int winSize = storage.windows.size();
        int expectedWinSize = storage.conf.historyWindowCount + 1 + storage.conf.futureWindowCount;
        Assert.assertEquals(expectedWinSize, winSize);
    }

    @Test
    public void testInsertsOutsideWindow() {
        Storage storage = Storage.init();
        byte[] data = new byte[100];
        ThreadLocalRandom.current().nextBytes(data);


        long tstampInPast = System.currentTimeMillis();
        tstampInPast -= 10 * 24 * 3600 * 1000;

        long keyInPast = storage.addEntry(tstampInPast, data);
        Assert.assertEquals(0L, keyInPast);


        long tstampInFuture = System.currentTimeMillis();
        tstampInFuture += 10 * 24 * 3600 * 1000;

        long keyInFuture = storage.addEntry(tstampInFuture, data);
        Assert.assertEquals(0L, keyInFuture);
    }


    @Test
    public void testInsertsInEachWindow() {
        Storage storage = Storage.init();

        final long now = System.currentTimeMillis();
        HashMap<Long, byte[]> storedDat = new HashMap<>();

        for (int idx = -1 * storage.conf.historyWindowCount; idx <= storage.conf.futureWindowCount; idx++) {
            long tstamp = now + idx * TimeUnit.DAYS.toMillis(1);
            byte[] data = new byte[100];
            ThreadLocalRandom.current().nextBytes(data);

            long key = storage.addEntry(tstamp, data);
            Assert.assertNotEquals(0, key);

            storedDat.put(key, data);
        }

        for (Entry<Long, byte[]> e: storedDat.entrySet()) {
            Long key = e.getKey();
            byte[] orgData = e.getValue();

            byte[] readData = storage.getEntry(key);
            Assert.assertArrayEquals(orgData, readData);
        }
    }


    @Data @AllArgsConstructor
    static class Event {
        long tstamp;
        String data;
    }

    static class EventSerDes implements SerDes<Event> {
        public byte[] marshall(Event val) {
            return ByteBuffer.allocate(val.data.length() + 8)
                           .putLong(val.tstamp)
                           .put(val.data.getBytes(UTF_8))
                           .array();
        }

        public Event unmarshall(byte[] data) {
            String strData = new String(data, 8,  data.length - 8, UTF_8);
            long tstamp = ByteBuffer.wrap(data).getLong();
            return new Event(tstamp, strData);
        }

        public long timestampOfT(Event val) {
            return val.tstamp;
        }
    }

    @Test
    public void testIterator() {

        final long oneMinute    = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
        final long twoMinutes   = TimeUnit.MILLISECONDS.convert(2, TimeUnit.MINUTES);

        long now = System.currentTimeMillis();
        long windowDuration = oneMinute;
        int  pastWindowCount = 2;
        int  futureWindowCount = 2;

        Function<Event, byte[]> key = (Event e) -> e.data.substring(0, 5).getBytes(UTF_8);

        BytesKeyedCacheFactory<Event> factory = new BytesKeyedCacheFactory<>();
        factory.setSerdes(new EventSerDes());
        factory.addKeyer("LEADING_FIVE_LETTERS", key);
        factory.setHistoryWindowsCount(pastWindowCount);
        factory.setFutureWindowCount(futureWindowCount);
        factory.setWindowTimespanMs(windowDuration);
        long start = factory.setStartTimeMillis(now);
        BytesCache<Event> cache = factory.getInstance();

        assertTrue(  cache.add( new Event(start, "Event now")));
        assertTrue(  cache.add( new Event(start + oneMinute,  "Event in one minute")));
        assertTrue(  cache.add( new Event(start + twoMinutes, "Event in two minutes")));
        assertTrue(  cache.add( new Event(start - oneMinute, "Event before one minute")));
        assertTrue(  cache.add( new Event(start - twoMinutes, "Event before two minutes")));

        EventSerDes serDes = new EventSerDes();

        Iterator<Event> storageIterator = cache.storage.iterator(new Peeker<Event>() {
            public Event peek(long objPos, byte[] bucket, int pos, int len) {
                return serDes.unmarshall(bucket, pos, len);
            }
        });


        boolean hasNext = storageIterator.hasNext();
        Event event1 = storageIterator.next();
        assertEquals("Event before two minutes", event1.data);

        boolean hasNext2 = storageIterator.hasNext();
        Event event2 = storageIterator.next();
        assertEquals("Event before one minute", event2.data);

        boolean hasNext3 = storageIterator.hasNext();
        Event event3 = storageIterator.next();
        assertEquals("Event now", event3.data);

        boolean hasNext4 = storageIterator.hasNext();
        Event event4 = storageIterator.next();
        assertEquals("Event in one minute", event4.data);

        boolean hasNext5 = storageIterator.hasNext();
        Event event5 = storageIterator.next();
        assertEquals("Event in two minutes", event5.data);



        List<Event> events = cache.get("LEADING_FIVE_LETTERS", new Event(start, "Event"));

        HashSet<String> eventData = new HashSet<>();
        Iterator<Event> eventIterator = cache.values();
        while(eventIterator.hasNext()) {
            eventData.add(eventIterator.next().data);
        }

        Assert.assertEquals(events.size(), eventData.size());
        for (Event e: events) {
            String d = e.getData();
            assertTrue(eventData.contains(d));
        }
    }

}
