package mt.fireworks.timecache.large;

import lombok.AllArgsConstructor;
import lombok.Data;


@Deprecated
@Data @AllArgsConstructor
class MockObj {
    public final static int objSize = 2 * 8;

    long tstamp;
    long value;
    String tekst;

}
