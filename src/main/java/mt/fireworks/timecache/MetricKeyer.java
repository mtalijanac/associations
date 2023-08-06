package mt.fireworks.timecache;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class MetricKeyer<I, O> implements Function<I, O>, Metrics {
    @NonNull Function<I, O> delegate;
    @NonNull String name;

    final AtomicLong duration = new AtomicLong();
    final AtomicLong counter = new AtomicLong();

    public O apply(I obj) {
        long t = -System.nanoTime();
        try {
            O res = delegate.apply(obj);
            return res;
        }
        finally {
            t += System.nanoTime();
            duration.addAndGet(t);
            counter.incrementAndGet();
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String text() {
        return "## Keyer " + getName() + " metrics: \n"
             + TimeUtils.info(name, counter, duration);
    }

    @Override
    public String reset() {
        String ts = text();
        duration.set(0);
        counter.set(0);
        return ts;
    }

}
