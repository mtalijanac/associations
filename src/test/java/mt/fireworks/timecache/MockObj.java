package mt.fireworks.timecache;

import lombok.AllArgsConstructor;
import lombok.Data;


@Deprecated
@Data @AllArgsConstructor
public class MockObj {
    public final static int objSize = 2 * 8;

    long tstamp;
    long value;
    String tekst;

}
