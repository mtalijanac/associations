package mt.fireworks.associations.examples;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import lombok.AllArgsConstructor;
import lombok.Data;
import mt.fireworks.associations.cache.*;

/**
 * Example of keyer usage.
 *
 * Keyer is a {@code java.util.function.Function<T, R>} implementation.
 * It should return a 'key' for a value stored in cache.
 * Keyer should associate similar values. <br>
 *
 * In this example we are using TimeCache as http request storage.
 * Stored request are grouped by http methond and uri.
 */
public class Keyers {

    @Data @AllArgsConstructor
    static class HTTPRequest {
        long tstamp;
        String method;
        String uri;
        String payload;
    }

    @Test
    public void example() throws InterruptedException {
        //
        // Prepare cache with keyers which associate HTTPRequests based
        // on method and uri. Add few example HTTPReqeuest's.
        //
        Function<HTTPRequest, byte[]> method = req -> req.method.getBytes();
        Function<HTTPRequest, byte[]> uri = req -> req.uri.toString().getBytes();

        BytesCacheFactory<HTTPRequest> factory = new BytesCacheFactory<>();
        factory.setSerdes(new HTTPSerdes());
        factory.addKeyer("METHOD", method);
        factory.addKeyer("URI", uri);
        BytesCache<HTTPRequest> cache = factory.getInstance();



        // 2 POSTs and 1 GET, 2 Google and 1 Facebook
        long tstamp = System.currentTimeMillis();
        cache.add(new HTTPRequest(tstamp, "GET", "http://google.com/", "First request"));
        cache.add(new HTTPRequest(tstamp, "POST", "http://google.com/", "Second"));
        cache.add(new HTTPRequest(tstamp, "POST", "http://facebook.com/", "Third"));


        //
        // Lookup for associated requests based on methods and site
        //

        HTTPRequest fbPostQuery = new HTTPRequest(tstamp, "POST", "http://facebook.com/", "Post to Face");
        Map<String, List<HTTPRequest>> fbPostRes = cache.getAsMap(fbPostQuery);
        List<HTTPRequest> postRequests  = fbPostRes.get("METHOD");
        List<HTTPRequest> facebookRequests = fbPostRes.get("URI");

        Assert.assertEquals(2, postRequests.size());
        Assert.assertEquals(1, facebookRequests.size());


        HTTPRequest glDeleteQuery = new HTTPRequest(tstamp, "DELETE", "http://google.com/", "Delete from Google");
        Map<String, List<HTTPRequest>> glDeleteRes = cache.getAsMap(glDeleteQuery);
        List<HTTPRequest> deleteRequests  = glDeleteRes.get("METHOD");
        List<HTTPRequest> googleRequests = glDeleteRes.get("URI");

        Assert.assertEquals(0, deleteRequests.size());
        Assert.assertEquals(2, googleRequests.size());

    }



    static class HTTPSerdes implements CacheSerDes<HTTPRequest> {
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

