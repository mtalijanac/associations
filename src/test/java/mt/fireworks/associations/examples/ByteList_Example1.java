package mt.fireworks.associations.examples;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import mt.fireworks.associations.ByteList;
import mt.fireworks.associations.ByteList.DataIterator;

public class ByteList_Example1 {

    @Test
    public void ratata() {

        String[] quotes = {
                "In the end, it all comes down to 0 and 1",                     // ― Vineet Goel
                "Never trust a computer you can't throw out a window",          // ― Steve Wozniak
                "Windows 10 is reliably unreliable.",                           // ― Steven Magee
                "Code is like humor. When you have to explain it, it’s bad.",   // – Cory House"
                "Confusion is part of programming.",                            // ― Felienne Hermans
                "Software and cathedrals are much the same — first we build them, then we pray.", // - Anonymous
                "There is always one more bug to fix.",                         //  – Ellen Ullman
                "If, at first, you do not succeed, call it version 1.0.",       // ― Khayri R.R. Woulfe
        };


        ByteList endlessListOfBytes = new ByteList();


        //
        // Randomly select one million quotes, store them all to endlessListOfBytes.
        //

        int n = 1_000_000;
        int[] randomQuoteIndexes = new int[n];              // history of quote selection
        long[] storeIndexes = new long[n];                  // history of storage indexes

        for (int i = 0; i < n; i++) {
            int quoteIdx = ThreadLocalRandom.current().nextInt(quotes.length);
            String selectedQuote = quotes[quoteIdx];
            byte[] data = selectedQuote.getBytes(UTF_8);
            long storeIdx = endlessListOfBytes.add(data);

            randomQuoteIndexes[i] = quoteIdx;
            storeIndexes[i] = storeIdx;
        }


        //
        // We will iterate trough endlessListOfBytes using
        // methods: get, peek, iterator and forEach.
        //


        //
        // Get is simplest method: for index passed, stored array is returned.
        // In order to use get, indexes are stored in 'storeIndexes' array.
        //
        using_get: {
            for (int i = 0; i < n; i++) {
                long dataIdx = storeIndexes[i];
                byte[] data = endlessListOfBytes.get(dataIdx);
                String quoteFromList = new String(data, UTF_8);

                int quoteIdx = randomQuoteIndexes[i];
                String selectedQuote = quotes[quoteIdx];

                assertEquals(selectedQuote, quoteFromList);
            }
        }


        //
        // Peek is similar to get but uses Peeker interface to access data
        // Peeker can directly access bytes of storage, without
        // allocating intermediate byte array as when using get method.
        //
        using_peek: {
            for (int i = 0; i < n; i++) {
                long dataIdx = storeIndexes[i];
                String quoteFromList = endlessListOfBytes.peek(dataIdx,
                        (objPos, bucket, pos, len) -> new String(bucket, pos, len, UTF_8));

                int quoteIdx = randomQuoteIndexes[i];
                String selectedQuote = quotes[quoteIdx];

                assertEquals(selectedQuote, quoteFromList);
            }
        }


        //
        // Using dataIterator we can iterate trough list without using indexes.
        //
        using_iterator: {
            DataIterator<String> dataIterator = endlessListOfBytes.iterator(
                (objPos, bucket, pos, len) ->
                    new String(bucket, pos, len, UTF_8)
            );

            for (int i = 0; dataIterator.hasNext(); i++) {
                String quoteFromList = dataIterator.next();

                int quoteIdx = randomQuoteIndexes[i];
                String selectedQuote = quotes[quoteIdx];

                assertEquals(selectedQuote, quoteFromList);
            }
        }


        //
        // forEach is similar to dataIterator without
        //
        using_foreach: {

            AtomicInteger loopCounter = new AtomicInteger();

            endlessListOfBytes.forEach((objPos, bucket, pos, len) -> {
                String quoteFromList = new String(bucket, pos, len, UTF_8);

                int i = loopCounter.getAndIncrement();
                int quoteIdx = randomQuoteIndexes[i];
                String selectedQuote = quotes[quoteIdx];

                assertEquals(selectedQuote, quoteFromList);
                assertEquals(storeIndexes[i], objPos);
                return null;
            });
        }

    }
}
