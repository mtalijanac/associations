package mt.fireworks.timecache;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class MetricKeyer<I, O> implements Function<I, O>, Measureable {
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

    public String metricsTxt() {
        return toString();
    }

}
