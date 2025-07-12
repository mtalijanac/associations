package mt.fireworks.associations.examples;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import mt.fireworks.associations.CompactMap2;
import mt.fireworks.associations.SerDes;

public class CompactMap2_UsageExample1 {

    @Test
    public void usage_example() {
        SerDes<String> stringSerdes = new StringSerDes();
        Function<String, byte[]> keyer = this::stringKey;
        CompactMap2<String> compactMap = new CompactMap2<>(stringSerdes, keyer);

        //
        // basic additions to the map
        //
        byte[] appleKey = compactMap.add("apple");
        byte[] bananaKey = compactMap.add("banana");
        byte[] cherryKey = compactMap.add("cherry");

        System.out.println(compactMap.size()); // Output: 3

        String a = compactMap.get(appleKey); // This will return "apple"
        System.out.println(a); // Output: apple

        String b = compactMap.get(bananaKey);
        System.out.println(b); // Output: banana


        //
        // overwrite existing value within map
        //
        byte[] bananaSplitKey = compactMap.add("banana split");

        System.out.println(compactMap.size()); // Output: 3

        String c = compactMap.get(bananaSplitKey);
        System.out.println(c); // Output: banana split

        boolean res = Arrays.equals(bananaKey, bananaSplitKey);
        System.out.println(res); // Output: true

        // "banana split" and "banana" share the same key: BANAN
        // thus "banana split" removed "banana" from map


        //
        // other operations on compact map:
        //

        // containsKey methods check for presence of key in a map
        boolean withACherryOnTop = compactMap.containsKey("cherry on top");
        System.out.println(withACherryOnTop); // Output: true as it has 'cherry' in it


        // peek methods allows to access the value without unmarshalling it from the map
        compactMap.peek("cherry on top", (objPos, bucket, pos, len) -> {
            String peekeedValue = new String(bucket, pos, len, UTF_8);
            System.out.println(peekeedValue); // Outputs: cherry
            return peekeedValue;
        });


        // it has few iteration methods, for example, forEachKeyValue. Output:
        //   Key: CHERR, Value: cherry
        //   Key: APPLE, Value: apple
        //   Key: BANAN, Value: banana split
        compactMap.forEachKeyValue(new BiConsumer<byte[], String>() {
            public void accept(byte[] key, String value) {
                String keyString = new String(key, UTF_8);
                System.out.println("Key: " + keyString + ", Value: " + value);
            }
        });


        //
        // map doesn't really removes objects, it just makes their bytes unaccsable
        // from internal index. In order to release memory occupied by dead objects
        // we need to call compact() method.
        //

        compactMap.remove("apple"); // remove "apple" from map
        compactMap.remove("BANANA FLAVORED CANDY"); // remove "banana split" from map
        compactMap.remove(cherryKey); // remove "cherry" from map

        compactMap.compact();

        System.out.println(compactMap.size()); // Output: 0, as all objects were removed);
    }


    byte[] stringKey(String val) {
        String upperCase = StringUtils.substring(val, 0, 5).toUpperCase();
        byte[] key = upperCase.getBytes(UTF_8);
        return key;
    }


    static class StringSerDes implements SerDes<String> {

        @Override
        public byte[] marshall(String val) {
            return val.getBytes(UTF_8);
        }

        @Override
        public String unmarshall(byte[] data) {
            return new String(data, UTF_8);
        }

        // optional but much more efficient than unmarshall(byte[] data)
        @Override
        public String unmarshall(byte[] data, int position, int length) {
            return new String(data, position, length, UTF_8);
        }
    }

}
