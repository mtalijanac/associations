package mt.fireworks.timecache.storage;

import java.util.concurrent.atomic.AtomicLong;

class TimeUtils {

    static String toReadable(long nano) {
        if (nano < 1_000l) return nano + " ns";
        if (nano < 1_000_000l) return (nano / 1_000l) + " μs";
        if (nano < 1_000_000_000l) return (nano / 1_000_000l) + " ms";
        return String.format("%.2f s", (double) nano / 1_000_000_000d);
    }


    static String info(String name, AtomicLong countRef, AtomicLong timeRef) {
        long count = countRef.get();
        long time = timeRef.get();
        String sec = toReadable(time);
        double speed = (double) count * 1_000_000_000d / time;
        String res = name + ": " + count + ", dur: " + sec + " [" + speed + " per sec]";
        return res;
    }

}
