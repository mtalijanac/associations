package mt.fireworks.associations.examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.Test;

import mt.fireworks.associations.Associations;
import mt.fireworks.associations.BytesMap;


/**
 * <h1>Example 1: Counting Words, Tracking Punctuation</h1>
 *
 * This example will show basics of data association.<p>
 *
 * BytesMap is used to store quotes. Quotes are associated
 * by their word count and ending punctuation. Few queries
 * are run fetching quotes by their associations.
 */
public class BytesMap_Example1 {

    @Test
    public void counting_words__tracking_punctuation() {

        String[] quotes = {
            "Who am I to judge?",                                  // - Pope Francis
            "The only thing we have to fear is fear itself.",      // - Franklin D. Roosevelt
            "I have a dream today!",                               // - Martin Luther King Jr.
            "The only true wisdom is in knowing you know nothing.",// - Socrates
            "Carpe diem!",                                         // - Horace
            "To be or not to be?",                                 // - William Shakespeare
            "Where there is love, there is life.",                 // - Mahatma Gandhi
        };

        Function<String, byte[]> wordCounter = quote -> {
            int lenght = quote.split("\\s").length;
            return BigInteger.valueOf(lenght).toByteArray();
        };

        Function<String, byte[]> endingPunctutation = quote -> {
            char ending = quote.charAt(quote.length() - 1);
            return new byte[] {(byte) ending};
        };

        BytesMap<String> map = BytesMap.newInstance(String.class)
                .withSerdes(Associations.stringSerDes())
                .associate("word_count", wordCounter)
                .associate("ending", endingPunctutation)
                .allocationSize(64 * 1024)
                .build();

        for (String q: quotes)
            map.add(q);



        //
        // Fetch questions
        //
        List<String> questions = map.get("ending", "?");
        assertTrue(questions.contains("To be or not to be?"));
        assertTrue(questions.contains("Who am I to judge?"));
        assertEquals(2, questions.size());



        //
        // Fetch quotes of same word count
        //
        List<String> fiveWordedQuotes = map.get("word_count", /*Ghandi:*/ "Change the world by example.");
        assertTrue(fiveWordedQuotes.contains("Who am I to judge?"));
        assertTrue(fiveWordedQuotes.contains("I have a dream today!"));
        assertEquals(2, fiveWordedQuotes.size());



        //
        // Fetch ALL associations of given query:
        //
        Map<String, List<String>> asGreene = map.getAsMap(/*Graham Greene:*/ "Failure too is a form of death.");
        List<String> sevenWorded = asGreene.get("word_count");
        List<String> fullStops = asGreene.get("ending");

        assertTrue(sevenWorded.contains("Where there is love, there is life."));
        assertEquals(1, sevenWorded.size());

        assertTrue(fullStops.contains("Where there is love, there is life."));
        assertTrue(fullStops.contains("The only true wisdom is in knowing you know nothing."));
        assertTrue(fullStops.contains("The only thing we have to fear is fear itself."));
        assertEquals(3, fullStops.size());



        //
        // Add new quote, and then fetch associations using the quote as query
        //
        Map<String, List<String>> asLarry = map.addAndGet(/*Larry Winget:*/ "Act now!");
        List<String> exclamations = asLarry.get("ending");
        List<String> twoWordedQuotes = asLarry.get("word_count");

        assertTrue(exclamations.contains("Act now!"));
        assertTrue(exclamations.contains("Carpe diem!"));
        assertTrue(exclamations.contains("I have a dream today!" ));
        assertEquals(3, exclamations.size());

        assertTrue(twoWordedQuotes.contains("Act now!"));
        assertTrue(twoWordedQuotes.contains("Carpe diem!"));
        assertEquals(2, twoWordedQuotes.size());
    }


}
