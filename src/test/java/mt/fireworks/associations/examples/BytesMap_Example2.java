package mt.fireworks.associations.examples;

import static org.junit.Assert.assertArrayEquals;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.junit.Test;

import mt.fireworks.associations.BytesMap;

/**
 * <h1>Example 2: Using multiple indexes</h1>
 *
 * In this example map contains numbers 1 to 20. Stored numbers
 * are grouped in two indexes by divisibility with 3 and 5.<p>
 *
 */
public class BytesMap_Example2 {

    @Test
    public void congruent_numbers() {

        //
        // Two numbers, a and b, are congruent in "modulo n" when
        // they have same remainder when divided by n:
        //
        //     a ≡ b (mod n)     <-->    a % n == b % n
        //

        Function<Integer, byte[]> mod3 = (Integer n) -> {
            return BigInteger.valueOf(n % 3).toByteArray();
        };

        Function<Integer, byte[]> mod5 = (Integer n) -> {
            return BigInteger.valueOf(n % 5).toByteArray();
        };

        BytesMap<Integer> map = BytesMap.newInstance(Integer.class)
                .usingMarshaller((Integer val) -> BigInteger.valueOf(val).toByteArray())
                .usingUnmarshaller((byte[] data) -> new BigInteger(data).intValue())
                .associate("modulo 3", mod3)
                .associate("modulo 5", mod5)
                .build();

        for (int num = 1; num <= 20; num++)
            map.add(num);



        //
        // Test association of numbers based on modulo 3
        //

        modulo_3: {
            Integer[][] mod3ExpectedGrouping = {
                {3, 6, 9, 12, 15, 18},			// this group is divisible by 3
                {1, 4, 7, 10, 13, 16, 19},      // this group has reminder 1
                {2, 5, 8, 11, 14, 17, 20},      // this group has reminder 2
            };

            List<Integer> congruentTo3 = map.get("modulo 3", 3);
            assertArrayEquals(mod3ExpectedGrouping[0], congruentTo3.toArray());

            List<Integer> congruentTo1 = map.get("modulo 3", 1);
            assertArrayEquals(mod3ExpectedGrouping[1], congruentTo1.toArray());

            List<Integer> congruentTo2 = map.get("modulo 3", 2);
            assertArrayEquals(mod3ExpectedGrouping[2], congruentTo2.toArray());


            //
            // BytesMap#indexAssocations is used to fetch iterator of all
            // associated data under given index. In this example numbers
            // are associated by remainder when divided with 3.
            //
            Iterator<List<Integer>> associationIterator = map.indexAssociations("modulo 3");
            while (associationIterator.hasNext()) {
                List<Integer> associatedValues = associationIterator.next();
                Integer[] congruentNumbers = associatedValues.toArray(new Integer[0]);

                int idx = congruentNumbers[0] % 3;
                Integer[] expected = mod3ExpectedGrouping[idx];

                assertArrayEquals(expected, congruentNumbers);
            }
        }


        //
        // Same as before but modulo 5
        //
        modulo_5: {
            Integer[][] mod5ExpectedGrouping = {
                    { 5, 10, 15, 20}, 		// this group is divisible by 5
                    { 1,  6, 11, 16}, 		// this group has reminder 1
                    { 2,  7, 12, 17}, 		// this group has reminder 2
                    { 3,  8, 13, 18}, 		// this group has reminder 3
                    { 4,  9, 14, 19}, 		// this group has reminder 4
            };

            List<Integer> congruentTo5 = map.get("modulo 5", 5);
            assertArrayEquals(mod5ExpectedGrouping[0], congruentTo5.toArray());

            List<Integer> congruentTo1 = map.get("modulo 5", 1);
            assertArrayEquals(mod5ExpectedGrouping[1], congruentTo1.toArray());

            List<Integer> congruentTo2 = map.get("modulo 5", 2);
            assertArrayEquals(mod5ExpectedGrouping[2], congruentTo2.toArray());

            List<Integer> congruentTo3 = map.get("modulo 5", 3);
            assertArrayEquals(mod5ExpectedGrouping[3], congruentTo3.toArray());

            List<Integer> congruentTo4 = map.get("modulo 5", 4);
            assertArrayEquals(mod5ExpectedGrouping[4], congruentTo4.toArray());


            //
            // BytesMap#indexAssocations is used to fetch iterator of all
            // associated data under given index. In this example numbers
            // are associated by remainder when divided with 5.
            //
            Iterator<List<Integer>> associationIterator = map.indexAssociations("modulo 5");
            while (associationIterator.hasNext()) {
                List<Integer> associatedValues = associationIterator.next();
                Integer[] congruentNumbers = associatedValues.toArray(new Integer[0]);

                int idx = congruentNumbers[0] % 5;
                Integer[] expected = mod5ExpectedGrouping[idx];

                assertArrayEquals(expected, congruentNumbers);
            }
        }
    }

}
