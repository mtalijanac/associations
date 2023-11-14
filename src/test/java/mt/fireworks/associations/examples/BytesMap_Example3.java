package mt.fireworks.associations.examples;

import static org.junit.Assert.*;

import java.math.BigInteger;
import java.util.*;
import java.util.function.Function;

import org.junit.Test;

import mt.fireworks.associations.Associations;
import mt.fireworks.associations.BytesMap;

/**
 * <h1>Example 3: Advanced iterators</h1>
 *
 * This examples will show advance usage of associated data.<br>
 *
 * Bytes map is used to store quotes. Quotes are associated by occurrence of words: love and fear.
 *
 */
public class BytesMap_Example3 {


    @Test
    public void iterations() {

        String[] loveFearWisdomQuotes = {
                "Love is life. And if you miss love, you miss life.",                                 // - Leo Buscaglia
                "The best thing to hold onto in life is each other.",                                 // - Audrey Hepburn
                "In love, there is only the giving and receiving of pain.",                           // - Khalil Gibran
                "To love and be loved is to feel the sun from both sides.",                           // - David Viscott
                "Love is a canvas furnished by nature and embroidered by imagination.",               // - Voltaire
                "The only true wisdom is in knowing you know nothing.",                               // - Socrates
                "Turn your wounds into wisdom.",                                                      // - Oprah Winfrey
                "To conquer fear is the beginning of wisdom.",                                        // - Bertrand Russell
                "The only thing we have to fear is fear itself.",                                     // - Franklin D. Roosevelt
                "Fear is the path to the dark side.",                                                 // - Yoda
                "Fear is a reaction. Courage is a decision.",                                         // - Winston Churchill
                "In love, there is no fear; but perfect love casts out fear.",                        // - Bible
                "Love is what we were born with. Fear is what we learned here.",                      // - Marianne Williamson
                "The greatest wisdom is in simplicity. Love, respect, tolerance, sharing, gratitude." // - Meher Baba
        };


        Function<String, byte[]> love = quote -> {
            int index = quote.toLowerCase().indexOf("love");
            return index < 0 ? null : BigInteger.valueOf(index).toByteArray();
        };

        Function<String, byte[]> fear = quote -> {
            int index = quote.toLowerCase().indexOf("fear");
            return index < 0 ? null : BigInteger.valueOf(index).toByteArray();
        };


        BytesMap<String> map = BytesMap.newInstance(String.class)
                 .withSerdes(Associations.stringSerDes())
                 .associate("love", love)
                 .associate("fear", fear)
                 .build();

        for (String q: loveFearWisdomQuotes)
            map.add(q);


        //
        // Using BytesMap#values method we will iterate trough all stored quotes.
        // There are 11 quotes containing words 'love' and/or 'fear'.
        // Three quotes without words 'love' or 'fear' were never stored in map.
        //
        iterating_all_values: {
            List<String> storedQuotes = new ArrayList<>();
            Iterator<String> values = map.values();
            values.forEachRemaining(storedQuotes::add);
            assertEquals(11, storedQuotes.size());
        }


        //
        // Using BytesMap#indexValue method, we will iterate trough quotes stored
        // under given index. There are seven quotes with word 'love',
        // and six quotes with word 'fear'.
        //
        iterating_indexes: {
            List<String> loveQuotes = new ArrayList<>();
            Iterator<String> loveIterator = map.indexValues("love");
            loveIterator.forEachRemaining(loveQuotes::add);
            assertEquals(7, loveQuotes.size());

            List<String> fearQuotes = new ArrayList<>();
            Iterator<String> fearIterator = map.indexValues("fear");
            fearIterator.forEachRemaining(fearQuotes::add);
            assertEquals(6, fearQuotes.size());
        }


        iterating_keys: {
            List<String> keys = map.keys();
            assertTrue(keys.contains("love"));
            assertTrue(keys.contains("fear"));
        }


        //
        // Within each index, quotes are associated by position of word in quote.
        // Within love index, three quotes are starting with word 'love'.
        // And three additional quotes have word 'love' on third position.
        // Within fear index two quotes start with word 'fear'.
        //
        advanced_iterators: {
            List<String> loveStartingQuotes = map.get("love", "Love query");
            List<String> fearStartingQuotes = map.get("fear", "Fear query");
            List<String> asLoveQuotes = map.get("love", "As love");

            assertEquals(3, loveStartingQuotes.size()); // Love .....
            assertEquals(2, fearStartingQuotes.size()); // Fear ...
            assertEquals(3, asLoveQuotes.size());       // two "In love ..", one "To love..." quote

            //
            // Iterate trough all associated love quotes
            //
            Iterator<List<String>> loveIterator = map.indexAssociations("love");
            List<List<String>> loveAssociations = new ArrayList<>();
            loveIterator.forEachRemaining(loveAssociations::add);

            // We have three love associations:
            //   - 3 quotes have love on first place -> key is 0
            //   - 3 quotes have love on third place -> key is 3
            //   - 1 quote has love at 40th place    -> key is 40
            assertEquals(3, loveAssociations.size());

            //
            // Iterate trough all associated fear quotes
            //
            Iterator<List<String>> fearIterator = map.indexAssociations("fear");
            List<List<String>> fearAssociations = new ArrayList<>();
            fearIterator.forEachRemaining(fearAssociations::add);

            //
            // Out of six fear quotes, two start with Fear, and remaining 4 have word on distinct place.
            //
            assertEquals(5, fearAssociations.size());
        }

    }

}
