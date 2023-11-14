package mt.fireworks.associations.examples;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.Test;

import lombok.AllArgsConstructor;
import lombok.Data;
import mt.fireworks.associations.cache.BytesCache;
import mt.fireworks.associations.cache.CacheSerDes;

/**
 * <h1>Cache example 1: Keyer usage</h1>
 *
 * Keyer is a implementation of {@code java.util.function.Function<T, R>}.
 * It should return a 'key' for a value stored in cache. Different input
 * values, which share same key, are 'associated' values. <br>
 *
 * In this example we are storing http request like objects to a cache.
 * Stored request are associated by http method and by uri.
 */
public class Cache_Example1_Keyers {

    @Data @AllArgsConstructor
    static class HTTPRequest {
        long tstamp;
        String method;
        String uri;
        String payload;
    }

    @Test
    public void http_cache() throws InterruptedException {
        //
        // Prepare cache with keyers which associate HTTPRequests based
        // on method and uri. Add few example HTTPReqeuest's.
        //
        Function<HTTPRequest, byte[]> method = req -> req.method.getBytes();
        Function<HTTPRequest, byte[]> uri = req -> req.uri.toString().getBytes();

        BytesCache<HTTPRequest> cache = BytesCache.newInstance(HTTPRequest.class)
              .withSerdes(new HTTPSerdes())
              .associate("METHOD", method)
              .associate("URI", uri)
              .build();


        //
        // Add 2 POSTs and 1 GET, 2 Google and 1 Facebook
        //
        long tstamp = System.currentTimeMillis();
        cache.add(new HTTPRequest(tstamp,  "GET", "http://google.com/",   "First request"));
        cache.add(new HTTPRequest(tstamp, "POST", "http://google.com/",   "Second"));
        cache.add(new HTTPRequest(tstamp, "POST", "http://facebook.com/", "Third"));


        //
        // Lookup for associated request using get method
        //
        facbook_query: {
            HTTPRequest fbPostQuery = new HTTPRequest(tstamp, "POST", "http://facebook.com/", "Post to Face");

            List<HTTPRequest> postRequests = cache.get("METHOD", fbPostQuery);
            assertEquals(2, postRequests.size());

            List<HTTPRequest> facebookRequests = cache.get("URI", fbPostQuery);
            assertEquals(1, facebookRequests.size());
        }


        //
        // Lookup using getAsMap method
        //
        google_query: {
            HTTPRequest glDeleteQuery = new HTTPRequest(tstamp, "DELETE", "http://google.com/", "Delete from Google");

            Map<String, List<HTTPRequest>> glDeleteRes = cache.getAsMap(glDeleteQuery);
            List<HTTPRequest> deleteRequests  = glDeleteRes.get("METHOD");
            List<HTTPRequest> googleRequests = glDeleteRes.get("URI");

            assertEquals(0, deleteRequests.size());
            assertEquals(2, googleRequests.size());
        }
    }



    public static class HTTPSerdes implements CacheSerDes<HTTPRequest> {
        public byte[] marshall(HTTPRequest val) {
            String res = val.tstamp + "\n"
                       + val.method + "\n"
                       + val.uri + "\n"
                       + val.payload;
            return res.getBytes();
        }

        public HTTPRequest unmarshall(byte[] data) {
            String res = new String(data);
            String[] spl = res.split("\n");
            long tstamp = Long.parseLong(spl[0]);
            return new HTTPRequest(tstamp, spl[1], spl[2], spl[3]);
        }

        public long timestampOfT(HTTPRequest val) {
            return val.tstamp;
        }
    }

}

