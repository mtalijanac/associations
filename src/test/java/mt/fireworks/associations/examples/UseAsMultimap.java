package mt.fireworks.associations.examples;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;

import org.junit.Test;

import lombok.AllArgsConstructor;
import lombok.Data;
import mt.fireworks.associations.cache.*;

/**
 * Simplest example of cache usage. Add bunch of simple Events to cache.
 * Fetch back associated events. Print output and assert correctness.
 * Ignore all time logic, and use cache as simple multimap.
 */
public class UseAsMultimap {

    @Data @AllArgsConstructor
    static class Event {
        long tstamp;
        String data;
    }


    @Test
    public void example() throws InterruptedException {
        //
        // Initialize cache using default values,
        // associate events by starting letters
        //
        Function<Event, byte[]> twoLetterKey  = (Event e) -> e.data.substring(0, 2).getBytes(UTF_8);
        Function<Event, byte[]> fourLetterKey = (Event e) -> e.data.substring(0, 4).getBytes(UTF_8);

        BytesCacheFactory<Event> factory = new BytesCacheFactory<>();
        factory.setSerdes(new EventSerDes());
        factory.addKeyer("TWO_LETTERS", twoLetterKey);
        factory.addKeyer("FOUR_LETTERS", fourLetterKey);
        BytesCache<Event> cache = factory.getInstance();


        //
        // Add bunch of test data
        //
        long tstamp = System.currentTimeMillis();
        Random rng = new Random(); // add some timestamp variation

        cache.add(new Event(tstamp += rng.nextInt(20), "Hello world"));
        cache.add(new Event(tstamp += rng.nextInt(20), "Hello my love"));
        cache.add(new Event(tstamp += rng.nextInt(20), "Hell isn't a good place"));
        cache.add(new Event(tstamp += rng.nextInt(20), "Helsinki"));
        cache.add(new Event(tstamp += rng.nextInt(20), "He-Man"));
        cache.add(new Event(tstamp += rng.nextInt(20), "Hi!!!"));
        cache.add(new Event(tstamp += rng.nextInt(20), "Hirogami"));


        //
        // Fetch events associated to a: "He", "Hell", "Hi" and "Hill"
        //
        Event hell_event = new Event(System.currentTimeMillis(), "Hellen of Troy");
        Map<String, List<Event>> hellAssociated = cache.getAsMap(hell_event);

        Event hill_event = new Event(System.currentTimeMillis(), "Hill by a house");
        Map<String, List<Event>> hillAssociated = cache.getAsMap(hill_event);


        //
        // Pretty print output, and assert correctness
        //
        // System.out.println(hellAssociated);
        // System.out.println(hillAssociated);

        // order of entries in response is determined by order of keyers
        assertEquals(2, hellAssociated.size());
        assertEquals(5, hellAssociated.get("TWO_LETTERS").size()); // 5 events start with 'He'
        assertEquals(3, hellAssociated.get("FOUR_LETTERS").size()); // 3 events start with 'Hell'

        assertEquals(2, hillAssociated.size());
        assertEquals(2, hillAssociated.get("TWO_LETTERS").size()); // 2 events start with 'Hi'
        assertEquals(0, hillAssociated.get("FOUR_LETTERS").size()); // 0 events start with 'Hi'
    }


    static class EventSerDes implements CacheSerDes<Event> {
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
