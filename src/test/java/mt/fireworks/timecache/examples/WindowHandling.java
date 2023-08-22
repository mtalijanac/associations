package mt.fireworks.timecache.examples;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
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
 * Duration of window is set to one minute. In this example cache  is
 * organized in two past windows, one current window, and two future windows. <br>
 *
 * Events are stored to matching window based on their tstamp. If event tstamp
 * is too old or is too new, and matching window can not be found, event will
 * be ignored and not stored to cache. <br>
 *
 * Cache time passing is invoked by invoking {@code Cache#tick()}. Thick will
 * shift all windows one step in past, and newest window will be added.
 * Oldest window will be removed, and stored events removed. <br>
 *
 * This example will start by adding five events, one for each cache window.
 * As time passes, by calling {@code Cache#tick()}, oldest events will be
 * forgotten. After  ticking five times no more events will be stored in the cache.
 */
public class WindowHandling {

    @Data @AllArgsConstructor
    static class Event {
        long tstamp;
        String data;
    }

    @Test
    public void usageExample() {
        final long oneMinute    = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
        final long twoMinutes   = TimeUnit.MILLISECONDS.convert(2, TimeUnit.MINUTES);
        final long threeMinutes = TimeUnit.MILLISECONDS.convert(3, TimeUnit.MINUTES);

        //
        // Initialize cache using two past and two future windows of one minute duration.
        // Associate events by leading letters
        //
        Function<Event, byte[]> key = (Event e) -> e.data.substring(0, 5).getBytes(UTF_8);

        long now = System.currentTimeMillis();
        long windowDuration = oneMinute;
        int  pastWindowCount = 2;
        int  futureWindowCount = 2;

        BytesKeyedCacheFactory<Event> factory = new BytesKeyedCacheFactory<>();
        factory.setSerdes(new EventSerDes());
        factory.addKeyer("LEADING_FIVE_LETTERS", key);
        factory.storageConf(pastWindowCount, futureWindowCount, windowDuration);
        long start = factory.setStartTimeMillis(now);
        BytesKeyedCache<Event> cache = factory.getInstance();

        // startTimestamp is rounded to lowest second
        assertEquals(now / 1000l * 1000l, start);


        //
        // Add events to cache
        //

        // add event in current time window
        assertTrue(  cache.add( new Event(start, "Event now")));

        // add couple future events
        assertTrue(  cache.add( new Event(start + oneMinute,  "Event in one minute")));
        assertTrue(  cache.add( new Event(start + twoMinutes, "Event in two minutes")));

        // but ignore one too far in future:
        assertFalse( cache.add( new Event(start + threeMinutes, "Event in three minutes")));

        // add couple past events
        assertTrue(  cache.add( new Event(start - oneMinute, "Event before one minute")));
        assertTrue(  cache.add( new Event(start - twoMinutes, "Event before two minutes")));

        // but ignore one too far in past:
        assertFalse( cache.add( new Event(start - threeMinutes, "Event before three minutes")));


        //
        // Repeatedly query for stored events while incrementing time
        //

        Event query = new Event(System.currentTimeMillis(), "Event query");

        // query for events, 5 stored events should be returned
        Map<String, List<Event>> res = cache.getAsMap(query);
        List<Event> events = res.get("LEADING_FIVE_LETTERS");
        assertEquals(5, events.size());

        // move cache windows a step in future, oldest window should be removed,
        // one event stored in that window should be forgotten
        cache.tick();
        res = cache.getAsMap(query);
        events = res.get("LEADING_FIVE_LETTERS");
        assertEquals(4, events.size());

        // Repeat
        cache.tick();
        res = cache.getAsMap(query);
        events = res.get("LEADING_FIVE_LETTERS");
        assertEquals(3, events.size());

        // Repeat
        cache.tick();
        res = cache.getAsMap(query);
        events = res.get("LEADING_FIVE_LETTERS");
        assertEquals(2, events.size());

        // Repeat
        cache.tick();
        res = cache.getAsMap(query);
        events = res.get("LEADING_FIVE_LETTERS");
        assertEquals(1, events.size());

        // Finally, tick once again, for all events being forgotten..
        cache.tick();
        res = cache.getAsMap(query);
        assertTrue(res.get("LEADING_FIVE_LETTERS").isEmpty());
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
