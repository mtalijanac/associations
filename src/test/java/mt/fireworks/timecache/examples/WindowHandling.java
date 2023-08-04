package mt.fireworks.timecache.examples;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.Test;

import lombok.AllArgsConstructor;
import lombok.Data;
import mt.fireworks.timecache.*;

/**
 * Example of handling time. <br>
 *
 * Cache in this example will group events in five time slots aka. windows.
 * Duration of window is set to one minute. Cache is are organized in two
 * past windows, one current window, and two future windows. <br>
 *
 * Events are stored to matching window, or are ignored if they are too old,
 * or too new. <br>
 *
 * As time passes cache will {@code tick}, shifting all windows one step in
 * past. Oldest window will be removed, and new future one will be added. <br>
 *
 * This example will at start add five events, one for each cache window.
 * As time passes, indicated by calling {@code Cache#tick()}, oldest events
 * will be forgotten as their window is removed from cache. By ticking five
 * times no more events will bes stored in cache.
 */
public class WindowHandling {

    @Data @AllArgsConstructor
    static class Event {
        long tstamp;
        String data;
    }

    @Test
    public void usageExample() {
        final long oneMinute = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

        //
        // Initialize cache using default values,
        // associate events by leading letters
        //
        Function<Event, byte[]> fourLetterKey = (Event e) -> e.data.substring(0, 4).getBytes(UTF_8);

        long now = System.currentTimeMillis();
        long windowDuration = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
        int  pastWindowCount = 2;
        int  futureWindowCount = 2;

        ByteCacheFactory<Event> factory = new ByteCacheFactory<>();
        factory.setSerdes(new EventSerDes());
        factory.addKeyers(fourLetterKey);
        factory.storageConf(pastWindowCount, futureWindowCount, windowDuration);
        long start = factory.setStartTimestamp(now); // startTimestamp is rounded to lowest second

        assertEquals(now / 1000l * 1000l, start);

        ByteCacheImpl<Event> cache = factory.getInstance();

        // add event in current time window
        cache.add(new Event(start, "Event now"));

        // add couple future events
        assertTrue(  cache.add( new Event(start + 1 * oneMinute, "Event in one minute")));
        assertTrue(  cache.add( new Event(start + 2 * oneMinute, "Event in two minutes")));

        // but ignore one too far in future:
        assertFalse( cache.add( new Event(start + 3 * oneMinute, "Event in three minutes")));

        // add couple past events
        assertTrue(  cache.add( new Event(start - 1 * oneMinute, "Event before one minute")));
        assertTrue(  cache.add( new Event(start - 2 * oneMinute, "Event before two minutes")));

        // but ignore one too far in past:
        assertFalse( cache.add( new Event(start - 3 * oneMinute, "Event before three minutes")));


        Event query = new Event(System.currentTimeMillis(), "Event query");

        // query for events, 5 stored events should be returned
        List<Entry<byte[], List<Event>>> res = cache.get(query);
        List<Event> events = res.get(0).getValue();
        assertEquals(5, events.size());

        // move cache windows a step in future, oldest window should be removed,
        // one event stored in that window should be forgotten
        cache.tick();
        res = cache.get(query);
        events = res.get(0).getValue();
        assertEquals(4, events.size());

        // Repeat
        cache.tick();
        res = cache.get(query);
        events = res.get(0).getValue();
        assertEquals(3, events.size());

        // Repeat
        cache.tick();
        res = cache.get(query);
        events = res.get(0).getValue();
        assertEquals(2, events.size());

        // Repeat
        cache.tick();
        res = cache.get(query);
        events = res.get(0).getValue();
        assertEquals(1, events.size());

        // Finally, all events should be forgotten..
        cache.tick();
        res = cache.get(query);
        assertTrue(res.isEmpty());
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
}
