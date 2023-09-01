package mt.fireworks.associations.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.Test;

import mt.fireworks.associations.BytesMap;
import mt.fireworks.associations.StringSerDes;

public class UsageExamples {


    //
    // Quotes are stored to map and organized by their word count
    // and ending punctuation. Queries are
    //

    @Test
    public void countingWords_trackingPunctuation() {

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
                .withSerdes(new StringSerDes())
                .associate("word_count", wordCounter)
                .associate("ending", endingPunctutation)
                .allocationSize(64 * 1024)
                .build();

        for (String q: quotes)
            map.add(q);


        //
        // Fetch associations by key name and query value:
        //
        List<String> questions = map.get("ending", "?");
        assertTrue(questions.contains("To be or not to be?"));
        assertTrue(questions.contains("Who am I to judge?"));
        assertEquals(2, questions.size());

        List<String> fiveWordedQuotes = map.get("word_count", /*Ghandi:*/ "Change the world by example.");
        assertTrue(fiveWordedQuotes.contains("Who am I to judge?"));
        assertTrue(fiveWordedQuotes.contains("I have a dream today!"));
        assertEquals(2, fiveWordedQuotes.size());


        //
        // Fetch ALL associations for given query:
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
        // Add a value, and than fetch associations using the value as query
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


    String[] loveFearWisdomQuotes = {
            "Love is life. And if you miss love, you miss life.", // - Leo Buscaglia
            "The best thing to hold onto in life is each other.", // - Audrey Hepburn
            "In love, there is only the giving and receiving of pain.", // - Khalil Gibran
            "To love and be loved is to feel the sun from both sides.", // - David Viscott
            "Love is a canvas furnished by nature and embroidered by imagination.", // - Voltaire
            "The only true wisdom is in knowing you know nothing.", // - Socrates
            "Turn your wounds into wisdom.", // - Oprah Winfrey
            "To conquer fear is the beginning of wisdom.", // - Bertrand Russell
            "The only thing we have to fear is fear itself.", // - Franklin D. Roosevelt
            "Fear is the path to the dark side.", // - Yoda
            "Fear is a reaction. Courage is a decision.", // - Winston Churchill
            "In love, there is no fear; but perfect love casts out fear.", // - Bible
            "Love is what we were born with. Fear is what we learned here.", // - Marianne Williamson
            "The greatest wisdom is in simplicity. Love, respect, tolerance, sharing, gratitude." // - Meher Baba
    };

}
