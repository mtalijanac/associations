package mt.fireworks.timecache.storage;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MetricKeyer<I, O> implements Function<I, O> {
    @NonNull Function<I, O> delegate;
    @NonNull String name;

    final AtomicLong duration = new AtomicLong();
    final AtomicLong counter = new AtomicLong();

    public O apply(I obj) {
        long t = -System.nanoTime();
        O res = delegate.apply(obj);
        t += System.nanoTime();
        duration.addAndGet(t);
        counter.incrementAndGet();
        return res;
    }

    public String toString() {
        String res = TimeUtils.info(name, counter, duration);
        return res;
    }

    public String resetMetrics() {
        String ts = toString();
        duration.set(0);
        counter.set(0);
        return ts;
    }

}