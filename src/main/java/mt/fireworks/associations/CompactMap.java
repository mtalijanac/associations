package mt.fireworks.associations;

import java.text.SimpleDateFormat;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.api.factory.primitive.ObjectLongHashingStrategyMaps;
import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;

import lombok.*;
import mt.fireworks.associations.cache.Metrics;

public class CompactMap<T> {

    // n + 1 lista, nikada ne mijenjaju index
    final ByteList[] segments;
    volatile int closedSegmentIndex;

    final SerDes<T> serdes;
    final Function<T, byte[]> keyer;
    final MutableObjectLongMap<byte[]> index;

    final float segmentWasteThreshold = 0.9f;
    final long  segmentSizeThresholdInBytes = 10 * 1024 * 1024; // 10 Mb

    @Getter
    final CompactMapMetrics metrics = new CompactMapMetrics();


    public CompactMap(SerDes<T> serdes, Function<T, byte[]> keyer) {
        this(3, serdes, keyer);
    }


    public CompactMap(int segmentCount, SerDes<T> serdes, Function<T, byte[]> keyer) {
        if (serdes instanceof Metrics) {
            this.serdes = serdes;
        }
        else {
            this.serdes = serdes.withMetric();
        }
        this.keyer = keyer;

        this.segments = new ByteList[segmentCount + 1];
        for (int idx = 0; idx < segmentCount; idx++) {
            this.segments[idx] = new ByteList();
        }

        this.closedSegmentIndex = segments.length - 1;
        this.index = ObjectLongHashingStrategyMaps.mutable
                        .of(new BytesHashingStrategy())
                        .asSynchronized();
    }


    /**
     * Add value to this map. <br>
     *
     * If key is never seen, a value is written in a random segment.
     * But for repeated entries of the same key, value is written to
     * the segment holding current key value, unless that segment is
     * is closed.
     */
    public boolean add(T value) {
        metrics.addCount.incrementAndGet();

        byte[] key = keyer.apply(value);
        byte[] data = serdes.marshall(value);

        // boolean contains = this.index.containsKey(key);
        this.index.updateValue(key, -1l, currentPointer -> {
            if (currentPointer == -1l)
                return addToSegment(rndIndex(), data);

            int segmentIndex = segementIndex(currentPointer);
            if (segmentIndex == this.closedSegmentIndex)
                return addToSegment(rndIndex(), data);

            return addToSegment(segmentIndex, data);
        });
        return true;
    }

    /** @return random non-closed segment index */
    int rndIndex() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int segmentIdx;
        do segmentIdx = rng.nextInt(segments.length);
        while (closedSegmentIndex == segmentIdx);
        return segmentIdx;
    }


    /** @return pointer to data/value stored at given segment */
    long addToSegment(int segmentIndex, byte[] data) {
        long listPosition = segments[segmentIndex].add(data);

        // calculate data pointer:
        long pointer = toPointer(segmentIndex, listPosition);
        return pointer;
    }


    public T get(T query) {
        byte[] key = keyer.apply(query);
        return get(key);
    }

    public T get(byte[] key) {
        metrics.getCount.incrementAndGet();

        long pointer = this.index.get(key);
        if (pointer == 0l) return null;

        int segementIndex = segementIndex(pointer);
        long position = (0x00FFFFFF_FFFFFFFFl) & pointer;

        ByteList byteList = segments[segementIndex];
        byte[] data = byteList.get(position);

        T result = byteList.peek(position, (objPos, bucket, pos, len) -> serdes.unmarshall(bucket, pos, len));
        return result;
    }


    /** @return pointer made out segment idx and position within it */
    long toPointer(int segmentIndex, long position) {
        long idxAtFirstByte = (segmentIndex & 0xFFl) << (7 * 8);
        long sevenBytesOfPosition = position & 0x00FFFFFF_FFFFFFFFl;
        long pointer = idxAtFirstByte | sevenBytesOfPosition;
        return pointer;
    }


    /** @return segment index to which this pointer points */
    int segementIndex(long pointer) {
        long v = pointer >>> (7 * 8);
        int idx = (int) v;
        return idx;
    }


    public void tick() {
        long start = System.nanoTime();
        metrics.tickCount.incrementAndGet();
        metrics.liveDataCopied.set(0);
        metrics.deadDataReleased.set(0);


        int skipIdx = this.closedSegmentIndex;
        for (int idx = 0; idx < segments.length; idx++) {
            if (idx == skipIdx) continue;

            // check if segment is big enough to care
            ByteList segment = this.segments[idx];
            long segmentSize = segment.getAllocatedSize();
            if (segmentSize <= segmentSizeThresholdInBytes)
                continue;

            compactSegment(idx);
        }

        long end = System.nanoTime();
        long dur = end - start;
        metrics.lastTickDuration.set(dur);
    }


    void compactSegment(final int oldSegmentIndex) {
        metrics.segmentsCompactedCount.incrementAndGet();

        final ByteList oldSegment = this.segments[oldSegmentIndex];

        // alociraj novi segment na praznom mjestu
        final ByteList newSegment = new ByteList();
        final int newSegmentIndex = closedSegmentIndex;
        this.segments[closedSegmentIndex] = newSegment;

        // zatvori segment koji sažimamo
        this.closedSegmentIndex = oldSegmentIndex;

        long usedSize = oldSegment.getUsedSize();
        AtomicLong liveDataSize = new AtomicLong();

        // žive podatke prekopiraj u novi segment
        oldSegment.forEach((position, bucket, off, len) -> {
            // pronađi ključ podatka i pointer za podatak
            // ako indeks za taj ključ sadrži isti pointer podatak je živ
            // i potrebno je kopirati podatak u novi segment

            long disPointer = toPointer(oldSegmentIndex, position);

            T value = serdes.unmarshall(bucket, off, len);
            byte[] key = keyer.apply(value);
            long livePointer = index.get(key);

            boolean isAlive = livePointer == disPointer;
            if (!isAlive) return value;     // dead value, ignore it

            liveDataSize.addAndGet(len);

            byte[] data = serdes.marshall(value);
            long newPointer = addToSegment(newSegmentIndex, data);
            index.put(key, newPointer);

            return value;
        });

        long lds = liveDataSize.get();
        long deadDataReleased = usedSize - lds;
        this.metrics.liveDataCopied.addAndGet(lds);
        this.metrics.deadDataReleased.addAndGet(deadDataReleased);
    }


    public void forEach(Procedure<T> on) {
        index.forEachKey(new Procedure<byte[]>() {
            public void value(byte[] key) {
                T value = get(key);
                if (value != null) {
                    on.accept(value);
                }
            }
        });
    }


    public String allMetrics() {

        long allocated = 0;
        long used = 0;
        for (ByteList seg: segments) {
            if (seg == null) continue;
            allocated += seg.getAllocatedSize();
            used += seg.getUsedSize();
        }

        StringBuilder sb = new StringBuilder();
        sb.append("## CompactMap metrics:\n");
        sb.append("    allocated: ..... ").append(allocated).append(" bytes\n");
        sb.append("         used: ..... ").append(used).append(" bytes\n");
        sb.append("  segment len: ..... ").append(segments.length).append("\n");
        sb.append("   index size: ..... ").append(index.size()).append("\n");


        sb.append(metrics.text(true));

        if (serdes instanceof Metrics) {
            sb.append("\n");
            sb.append((Metrics) serdes);
        }

        String res = sb.toString();
        return res;
    }

    @Data
    static class CompactMapMetrics implements Metrics {
        String name = "CompactMap";

        final AtomicLong addCount = new AtomicLong();
        final AtomicLong getCount = new AtomicLong();
        final AtomicLong tickCount = new AtomicLong();

        final AtomicLong segmentsCompactedCount = new AtomicLong();
        final AtomicLong liveDataCopied = new AtomicLong();
        final AtomicLong deadDataReleased = new AtomicLong();
        final AtomicLong lastTickDuration =  new AtomicLong();

        long metricStartTimeMs = System.currentTimeMillis();


        @Override
        public String text(boolean coments) {

            StringBuilder sb = new StringBuilder();

            {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                String startDate = sdf.format(metricStartTimeMs);
                sb.append("  Metrics running since: ").append(startDate).append("\n");

                long duration = System.currentTimeMillis() - metricStartTimeMs;

                long d = duration;

                long durationDays = d / (24*3600_000l);
                d -= (durationDays * 24*3600_000l);

                long durationHours = d / (3600_000l);
                d -= (durationHours * 3600_000l);

                long durationMins = d / 60_000l;
                d -= (durationMins * 60_000l);

                long durationSec = d / 1000l;
                d -= (durationSec * 1000l);

                sb.append("  Running for: ")
                  .append(durationDays).append(":")
                  .append(durationHours).append(":")
                  .append(durationMins).append(":")
                  .append(durationSec).append("\n");
            }


            sb.append("  addCount: ........ ").append(addCount).append("\n");
            sb.append("  getCount: ........ ").append(getCount).append("\n");
            sb.append("  tickCount: ....... ").append(tickCount).append("\n");
            sb.append("  segmentsCompacted: ").append(segmentsCompactedCount).append(" (total count of segments cleaned since begining)\n");
            sb.append("  lastTickDuration:  ").append(lastTickDuration).append(" ns\n");
            sb.append("  liveDataCopied: .. ").append(liveDataCopied).append(" bytes (data copied in last tick)\n");
            sb.append("  deadDataReleased:  ").append(deadDataReleased).append(" bytes (data remoed in last tick)\n");


            return sb.toString();
        }

        @Override
        public String reset() {
            addCount.set(0);
            getCount.set(0);
            tickCount.set(0);
            segmentsCompactedCount.set(0);
            metricStartTimeMs = System.currentTimeMillis();
            return text(true);
        }

    }
}
