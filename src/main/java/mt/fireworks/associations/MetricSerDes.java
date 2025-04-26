package mt.fireworks.associations;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import lombok.AllArgsConstructor;
import lombok.Getter;
import mt.fireworks.associations.cache.Metrics;
import mt.fireworks.associations.cache.TimeUtils;


@AllArgsConstructor
class MetricSerDes<T> implements SerDes<T>, Metrics {
    
    @Getter final String name = "SerDes";
    
    SerDes<T> delegate;

    
    final AtomicLong marshallCount = new AtomicLong();
    final AtomicLong marshallTime = new AtomicLong();
    
    @Override
    public byte[] marshall(T val) {
        return doTheFun(marshallCount, marshallTime, delegate::marshall, val);
    }
    
    
    final AtomicLong unmarshallCount = new AtomicLong();
    final AtomicLong unmarshallTime = new AtomicLong();

    @Override
    public T unmarshall(byte[] data) {
        return doTheFun(unmarshallCount, unmarshallTime, delegate::unmarshall, data);
    }

    
    <X,R> R doTheFun(AtomicLong count, AtomicLong timer, Function<X, R> job, X arg) {
        long d = -System.nanoTime();
        R res = job.apply(arg);
        d += System.nanoTime();
        count.incrementAndGet();
        timer.addAndGet(d);
        return res;
    }
    
    
    final AtomicLong inPlaceUnmarshallCount = new AtomicLong();
    final AtomicLong inPlaceUnmarshallTime = new AtomicLong();

    @Override
    public T unmarshall(byte[] data, int position, int length) {
        long d = -System.nanoTime();
        T res = delegate.unmarshall(data, position, length);
        d += System.nanoTime();
        inPlaceUnmarshallCount.incrementAndGet();
        inPlaceUnmarshallTime.addAndGet(d);
        return res;
    }
    
    
    public String toString() {
        String mar        = TimeUtils.info("     marshall", marshallCount, marshallTime);
        String unmar      = TimeUtils.info("   unmarshall", unmarshallCount, unmarshallTime);
        String inPlaceUnm = TimeUtils.info("   inPlaceUnm", inPlaceUnmarshallCount, inPlaceUnmarshallTime);

        String res = "## Serdes metrics:\n"
                   + mar + "\n"
                   + unmar + "\n"
                   + inPlaceUnm;
        return res;
    }
    

    @Override
    public String reset() {
        String res = toString();
        
        marshallCount.set(0);
        marshallTime.set(0);
        unmarshallCount.set(0);
        unmarshallTime.set(0);
        inPlaceUnmarshallCount.set(0);
        inPlaceUnmarshallTime.set(0);
        
        return res;
    }
    
    @Override
    public String text(boolean comments) {
        return toString();
    }

}
