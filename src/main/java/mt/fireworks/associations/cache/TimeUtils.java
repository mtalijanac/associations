package mt.fireworks.associations.cache;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

public class TimeUtils {

    private TimeUtils() {}

    public static String toReadable(long nano) {
        if (nano < 1_000l) return nano + " ns";
        if (nano < 1_000_000l) return (nano / 1_000l) + " μs";
        if (nano < 1_000_000_000l) return (nano / 1_000_000l) + " ms";
        return String.format("%.2f s", nano / 1_000_000_000d);
    }

    public static String info(String name, AtomicLong countRef, AtomicLong timeRef) {
        long count = countRef.get();
        long time = timeRef.get();
        String sec = toReadable(time);
        double speed = 1_000_000_000d * count / time;
        String speedStr = String.format("%.2f", speed);
        String res = name + ": " + count + ", dur: " + sec + " [" + speedStr + " per sec]";
        return res;
    }

    public static String readableTstamp(long tstamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String res = sdf.format(new Date(tstamp));
        return res;
    }
}
