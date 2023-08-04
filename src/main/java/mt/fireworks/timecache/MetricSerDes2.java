package mt.fireworks.timecache;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

import lombok.AllArgsConstructor;

/**
 * SerDes2 wrapper which does the metric.
 *
 */
@AllArgsConstructor
class MetricSerDes2<T> implements SerDes<T> {

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



    final AtomicLong timestampOfTCount = new AtomicLong();
    final AtomicLong timestampOfTTime = new AtomicLong();

    @Override
    public long timestampOfT(T val) {
        return doTheFun(timestampOfTCount, timestampOfTTime, delegate::timestampOfT, val);
    }



    final AtomicLong timestampOfDCount = new AtomicLong();
    final AtomicLong timestampOfDTime = new AtomicLong();

    @Override
    public long timestampOfD(byte[] data) {
        return doTheFun(timestampOfDCount, timestampOfDTime, delegate::timestampOfD, data);
    }



    final AtomicLong inPlaceTimestampOfDCount = new AtomicLong();
    final AtomicLong inPlaceTimestampOfDTime = new AtomicLong();

    @Override
    public long timestampOfD(byte[] data, int position, int length) {
        long d = -System.nanoTime();
        long tstamp = delegate.timestampOfD(data, position, length);
        d += System.nanoTime();
        inPlaceTimestampOfDCount.incrementAndGet();
        inPlaceTimestampOfDTime.addAndGet(d);
        return tstamp;
    }



    final AtomicLong equalsTCount = new AtomicLong();
    final AtomicLong equalsTTime = new AtomicLong();

    @Override
    public boolean equalsT(T val1, T val2) {
        return doTheBi(equalsTCount, equalsTTime, delegate::equalsT, val1, val2);
    }



    final AtomicLong equalsDCount = new AtomicLong();
    final AtomicLong equalsDTime = new AtomicLong();

    @Override
    public boolean equalsD(byte[] data1, byte[] data2) {
        return doTheBi(equalsDCount, equalsDTime, delegate::equalsD, data1, data2);
    }



    final AtomicLong inPlaceEqualsDCount = new AtomicLong();
    final AtomicLong inPlaceEqualsDTime = new AtomicLong();

    @Override
    public boolean equalsD(byte[] data1, int pos1, int len1, byte[] data2, int pos2, int len2) {
        long d = -System.nanoTime();
        boolean res = delegate.equalsD(data1, pos1, len1, data2, pos2, len2);
        d += System.nanoTime();
        inPlaceEqualsDCount.incrementAndGet();
        inPlaceEqualsDTime.addAndGet(d);
        return res;
    }


    <X,R> R doTheFun(AtomicLong count, AtomicLong timer, Function<X, R> job, X arg) {
        long d = -System.nanoTime();
        R res = job.apply(arg);
        d += System.nanoTime();
        count.incrementAndGet();
        timer.addAndGet(d);
        return res;
    }

    <X, Y,R> R doTheBi(AtomicLong count, AtomicLong timer, BiFunction<X, Y, R> job, X x, Y y) {
        long d = -System.nanoTime();
        R res = job.apply(x, y);
        d += System.nanoTime();
        count.incrementAndGet();
        timer.addAndGet(d);
        return res;
    }


    public String resetMetrics() {
        String ts = toString();

        marshallCount.set(0);
        marshallTime.set(0);
        unmarshallCount.set(0);
        unmarshallTime.set(0);
        inPlaceUnmarshallCount.set(0);
        inPlaceUnmarshallTime.set(0);
        timestampOfTCount.set(0);
        timestampOfTTime.set(0);
        timestampOfDCount.set(0);
        timestampOfDTime.set(0);
        inPlaceTimestampOfDCount.set(0);
        inPlaceTimestampOfDTime.set(0);
        equalsTCount.set(0);
        equalsTTime.set(0);
        equalsDCount.set(0);
        equalsDTime.set(0);
        inPlaceEqualsDCount.set(0);
        inPlaceEqualsDTime.set(0);

        return ts;
    }


    public String toString() {
        String mar        = info("  marshall", marshallCount, marshallTime);
        String unmar      = info("unmarshall", unmarshallCount, unmarshallTime);
        String inPlaceUnm = info("inPlaceUnm", inPlaceUnmarshallCount, inPlaceUnmarshallTime);
        String tstampT    = info("   tstampT", timestampOfTCount, timestampOfTTime);
        String tstampD    = info("   tstampD", timestampOfDCount, timestampOfDTime);
        String inPlaceTst = info("inPlaceTst", inPlaceTimestampOfDCount, inPlaceTimestampOfDTime);
        String equalsT    = info("   equalsT", equalsTCount, equalsTTime);
        String equalsD    = info("   equalsD", equalsDCount, equalsDTime);
        String inPlaceEqu = info("inPlaceEqu", inPlaceEqualsDCount, inPlaceEqualsDTime);

        String res = "Serdes metrics:\n"
                   + mar + "\n"
                   + unmar + "\n"
                   + inPlaceUnm + "\n"
                   + tstampT + "\n"
                   + tstampD + "\n"
                   + inPlaceTst + "\n"
                   + equalsT + "\n"
                   + equalsD + "\n"
                   + inPlaceEqu;
        return res;
    }

    String info(String name, AtomicLong countRef, AtomicLong timeRef) {
        long count = countRef.get();
        long time = timeRef.get();
        double sec = (double) time / 1_000_000_000d;
        double speed = (double) count / sec;
        String res = name + ": " + count + ", dur: " + sec + " s [" + speed + " per sec]";
        return res;
    }

}
