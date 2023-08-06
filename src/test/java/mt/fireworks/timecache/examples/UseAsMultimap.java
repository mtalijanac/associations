package mt.fireworks.timecache.examples;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.function.Function;

import org.junit.Test;

import lombok.AllArgsConstructor;
import lombok.Data;
import mt.fireworks.timecache.*;

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

        ByteCacheFactory<Event> factory = new ByteCacheFactory<>();
        factory.setSerdes(new EventSerDes());
        factory.addKeyer("TWO_LETTER", twoLetterKey);
        factory.addKeyer("FOUR_LETTER", fourLetterKey);
        ByteCacheImpl<Event> cache = factory.getInstance();


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
        List<Entry<byte[], List<Event>>> hellAssociated = cache.get(hell_event);

        Event hill_event = new Event(System.currentTimeMillis(), "Hill by a house");
        List<Entry<byte[], List<Event>>> hillAssociated = cache.get(hill_event);


        //
        // Pretty print output, and assert correctness
        //
        hellAssociated.forEach( System.out::println );
        hillAssociated.forEach( System.out::println );

        // order of entries in response is determined by order of keyers
        assertEquals(hellAssociated.size(), 2);
        assertEquals(hellAssociated.get(0).getValue().size(), 5); // 5 events start with 'He'
        assertEquals(hellAssociated.get(1).getValue().size(), 3); // 3 events start with 'Hell'

        assertEquals(hillAssociated.size(), 1);
        assertEquals(hillAssociated.get(0).getValue().size(), 2); // 2 events start with 'Hi'
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
