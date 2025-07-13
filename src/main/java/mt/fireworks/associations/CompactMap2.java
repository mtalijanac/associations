package mt.fireworks.associations;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.eclipse.collections.api.block.HashingStrategy;
import org.eclipse.collections.api.factory.primitive.ObjectLongHashingStrategyMaps;
import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;

import lombok.Cleanup;
import lombok.Data;
import mt.fireworks.associations.ByteList.Peeker;
import mt.fireworks.associations.cache.TimeUtils;

/**
 * A concurrent, memory-efficient data structure that stores serialized objects across multiple segments.
 *
 * <p>CompactMap2 is designed for high-throughput scenarios that require:
 * <ul>
 *   <li>Efficient memory usage</li>
 *   <li>Concurrent read/write operations</li>
 *   <li>Fast object lookup by key or query object</li>
 * </ul>
 *
 * <p>Objects are serialized and stored in segments, with an memory efficiency as
 * biggest priority. This map only grows, meaning that it doesn't remove old objects.
 * Instead it will compact segments and remove dead objects when user invokes {@link #compact()} method.
 * It is a form of GC, but instead being automatic it is initiated by the user.
 *
 * <h3>Basic Usage:</h3>
 * <pre>{@code
 * // Create with segment count, serializer and key function
 * SerDes<User> userSerializer = new UserSerializer();
 * Function<User, byte[]> keyFunction = User::getIdBytes;
 * CompactMap2<User> userMap = new CompactMap2<>(4, userSerializer, keyFunction);
 *
 * // Add objects
 * userMap.add(user);
 *
 * // Retrieve by query object
 * User found = userMap.get(new User(userId, null));
 *
 * // Retrieve by key directly
 * byte[] key = userId.getBytes();
 * User found = userMap.get(key);
 *
 * // Trigger compaction
 * userMap.compact();
 *
 * // Get metrics
 * String stats = userMap.metrics();
 * }</pre>
 *
 * @param <T> The type of objects stored in the map
 */
public class CompactMap2<T> {

    final private ByteList[] segments;
    final private ReentrantLock[] segmentLocks;
    final private int segmentAllocationSize;

    final private SerDes<T> serdes;
    final private Function<T, byte[]> keyer;
    final private MutableObjectLongMap<byte[]> index;

    final private CompactMap2Metrics metrics = new CompactMap2Metrics();

    private volatile int rwBarrier = 0;


    public CompactMap2(SerDes<T> serdes, Function<T, byte[]> keyer) {
        this(8, 1024 * 1024, serdes, keyer);
    }


    public CompactMap2(int segCount, int segAllocationSize, SerDes<T> serdes, Function<T, byte[]> keyer) {
        this(segCount, segAllocationSize, serdes, keyer, new BytesHashingStrategy());
    }


    public CompactMap2(
            int segCount, int segAllocationSize,
            SerDes<T> serdes, Function<T, byte[]> keyer,
            HashingStrategy<byte[]> hashingStrategy
    ) {
        this.segmentAllocationSize = segAllocationSize;
        this.segments = new ByteList[segCount + 1];
        for (int i = 0; i < segCount; i++)
            segments[i] = new ByteList(segAllocationSize);

        this.segmentLocks = new ReentrantLock[segCount + 1];
        for (int i = 0; i < this.segmentLocks.length; i++)
            segmentLocks[i] = new ReentrantLock();

        this.serdes = serdes;
        this.keyer = keyer;
        this.index = ObjectLongHashingStrategyMaps.mutable
                .of(hashingStrategy)
                .asSynchronized();
    }


    /**
     * It will create a new CompactMap2 with a modified keyer that returns a key,
     * with a hash prepended to it. This is usefull a performance trade-off where
     * keys aren't really unique, but their hash is. Cost is 4 bytes per key +
     * double key allocation as delegate keyer will copy the original key into
     * new array.
     */
    public static <T> CompactMap2<T> withHashedKeys(SerDes<T> serdes, Function<T, byte[]> keyer) {
        return withHashedKeys(8, 1024 * 1024, serdes, keyer);
    }

    public static <T> CompactMap2<T> withHashedKeys(
            int segCount, int segAllocationSize, SerDes<T> serdes, Function<T, byte[]> keyer
    ) {
        BytesWithHash bytesWithHash = new BytesWithHash();
        Function<T, byte[]> hashedKeyer = BytesWithHash.hashedKeyer(keyer);
        CompactMap2<T> map = new CompactMap2<>(segCount, segAllocationSize, serdes, hashedKeyer, bytesWithHash);
        return map;
    }


    /**
     * Add object to map. Return key of object in map.
     * Key is calculated by applying keyer on passed value.
     */
    public byte[] add(final T value) {
        final byte[] key = keyer.apply(value);
        put (key, value);
        return key;
    }

    public void put(final byte[] key, final T value) {
        final byte[] data = serdes.marshall(value);
        putBytes(key, data);
    }

    public void putBytes(final byte[] key, final byte[] marshalledT) {
        final int segIndex = lockWriteSegment();
        @Cleanup("unlock")
        final ReentrantLock segmentLock = segmentLocks[segIndex];
        final ByteList segment = segments[segIndex];
        final long objPos = segment.add(marshalledT);
        final long pointer = pointer(segIndex, objPos);
        index.put(key, pointer);

        rwBarrier++; // volatile write = release
    }


    final private AtomicInteger _segmentIndexCounter = new AtomicInteger(0);

    /**
     * Pick a segment to write to, lock it and return its index. This method is used
     * to ensure that writes are distributed across segments and that only one
     * thread can write to a segment at a time.
     */
    int lockWriteSegment() {
        while (true) {
            int idx = _segmentIndexCounter.getAndUpdate(operand -> (operand + 1) % segments.length);
            ByteList seg = segments[idx];
            if (seg == null) continue;
            ReentrantLock lock = segmentLocks[idx];
            boolean locked = lock.tryLock();
            if (locked) return idx;
        }
    }

    /** Returns number of stored objects in this map */
    public int size() {
        return index.size();
    }


    /** Query map for a associated object */
    public T get(T query) {
        metrics.totalGetQueryCount.incrementAndGet();
        byte[] key = keyer.apply(query);
        T res = peekWithKey(key, (objPos, bucket, pos, len) -> serdes.unmarshall(bucket, pos, len));
        return res;
    }

    /** Query map by key. */
    public T get(byte[] key) {
        metrics.totalGetKeyCount.incrementAndGet();
        T res = peekWithKey(key, (objPos, bucket, pos, len) -> serdes.unmarshall(bucket, pos, len));
        return res;
    }


    /** Peek into map for a value */
    public T peek(T query, Peeker<T> peeker) {
        byte[] key = keyer.apply(query);
        return peekWithKey(key, peeker);
    }


    public T peekWithKey(final byte[] key, final Peeker<T> peeker) {
        final int acquired = rwBarrier; // volatile read = acquire

        final long pointer = index.getIfAbsent(key, -1);
        if (pointer == -1) return null; // not found

        final T res = peekWithPointer(pointer, peeker);
        return res;
    }


    T peekWithPointer(final long pointer, final Peeker<T> peeker) {
        metrics.totalPeekCount.incrementAndGet();

        final int acquired = rwBarrier; // volatile read = acquire

        final int segementIndex = segementIndex(pointer);
        final long objPos = objPos(pointer);

        final ByteList segment = segments[segementIndex];
        if (segment == null) return null;

        final T res = segment.peek(objPos, peeker);
        return res;
    }


    /** @return true if map contains an associated object. */
    public boolean containsKey(T query) {
        byte[] key = keyer.apply(query);
        return index.containsKey(key);
    }

    /** @return true if map contains this key */
    public boolean containsKey(byte[] key) {
        return index.containsKey(key);
    }


    public void forEachKey(Consumer<byte[] /*key*/> on) {
        index.forEachKeyValue((key, pointer) -> on.accept(key));
    }

    public void forEachKeyValue(BiConsumer<byte[] /*key*/, T /*value*/> on) {
        index.forEachKeyValue((key, pointer) -> {
            T res = peekWithPointer(pointer, (objPos, bucket, pos, len) -> serdes.unmarshall(bucket, pos, len));
            on.accept(key, res);
        });
    }

    public void forEachValue(Consumer<T> on) {
        index.forEachKeyValue((key, pointer) -> {
            T res = peekWithPointer(pointer, (objPos, bucket, pos, len) -> serdes.unmarshall(bucket, pos, len));
            on.accept(res);
        });
    }


    public void remove(T query) {
        byte[] key = keyer.apply(query);
        remove(key);
    }

    public void remove(byte[] key) {
        index.removeKey(key);
    }



    final private ReentrantLock _compactionLock = new ReentrantLock();

    /**
     * Garbage collects the map by compacting all segments. This is a very expensive
     * operation which will release and again reallocate all memory for stored objects.
     * Only 'live' values are kept.
     *
     * <p>Operation is synchronized, to writes, so it is perfectly safe to
     * use compact map while reading/writing to it. However do except increased
     * cpu and memory usage and GC cycles during compaction.
     *
     * @return true if compaction was successful, false if it was cancelled
     */
    public boolean compact() {
        // find write to segment index
        // find read from segment index
        // allocate new bytelist store it under write segment
        // foreach value in read segment
        //   read data,
        //   calculate pointer and key
        //   get pointer from index
        //   if indexPointer and segmentPointer match
        //      write data to new segment
        //      write segmentPointer to index

        boolean locked = _compactionLock.tryLock();
        if (!locked) return false;
        @Cleanup("unlock") ReentrantLock unlock = _compactionLock;

        metrics.lastCompactObjectsSurvived.set(0);
        metrics.lastCompactObjectsDeleted.set(0);


        int writeSegmentIndex = -1;
        for (int idx = 0; idx < segments.length; idx++)
            if (segments[idx] == null) writeSegmentIndex = idx;

        final int initialEmptySegment = writeSegmentIndex;

        final long start = System.nanoTime();


        for (int readSegIdx = 0; readSegIdx < segments.length; readSegIdx++) {
            if (readSegIdx == initialEmptySegment) continue;
            compactOneSegment(readSegIdx, writeSegmentIndex);
            writeSegmentIndex = readSegIdx;
        }

        final long end = System.nanoTime();
        final long dur = end - start;

        metrics.totalCompactCount.incrementAndGet();
        metrics.totalCompactDurationNs.addAndGet(dur);
        metrics.lastCompactTimestamp.set(System.currentTimeMillis());
        metrics.lastCompactDurationNs.set(dur);

        return true;
    }


    void compactOneSegment(final int readSegIdx, final int writeSegIdx) {
        if (segments[writeSegIdx] != null) {
            throw new RuntimeException("Write segment is not empty: " + writeSegIdx);
        }

        final int acquired = rwBarrier; // volatile read = acquire

        // locks prevent writing to the segments being compacted

        @Cleanup("unlock")
        final ReentrantLock readLock = segmentLocks[readSegIdx];
        readLock.lock();

        @Cleanup("unlock")
        final ReentrantLock writeLock = segmentLocks[writeSegIdx];
        writeLock.lock();


        final ByteList readSegment = segments[readSegIdx];
        final ByteList writeSegment = new ByteList(segmentAllocationSize);
        segments[writeSegIdx] = writeSegment;

        readSegment.forEach((objPos, bucket, pos, len) -> {
            final T obj = serdes.unmarshall(bucket, pos, len);
            final byte[] key = keyer.apply(obj);

            final long segPointer = pointer(readSegIdx, objPos);
            final long oldPointer = index.getIfAbsent(key, -1);

            if (segPointer != oldPointer) {
                // pointer mismatch, skip this entry
                metrics.lastCompactObjectsDeleted.incrementAndGet();
                return null;
            }

            final long writeObjPos = writeSegment.add(bucket, pos, len);
            final long copyPointer = pointer(writeSegIdx, writeObjPos);
            rwBarrier++; // volatile write = release

            // if index was changed by another thread, use the updated version
            // else point index to new segment
            index.updateValue(key, copyPointer, currentIndexPointer ->
                currentIndexPointer == oldPointer ? copyPointer : currentIndexPointer
            );

            metrics.lastCompactObjectsSurvived.incrementAndGet();
            return obj;
        });

        segments[readSegIdx] = null;
    }


    @Deprecated
    public void tick() {
        compact();
    }


    /** @return pointer made out segment idx and position within it */
    long pointer(final int segmentIndex, final long objPos) {
        final long idxAtFirstByte = (segmentIndex & 0xFFl) << (7 * 8);
        final long sevenBytesOfPosition = objPos & 0x00FFFFFF_FFFFFFFFl;
        final long pointer = idxAtFirstByte | sevenBytesOfPosition;
        return pointer;
    }

    /** @return segment index to which this pointer points */
    int segementIndex(final long pointer) {
        final long v = pointer >>> (7 * 8);
        final int idx = (int) v;
        return idx;
    }

    /** @return objPosition of data within segment */
    long objPos(final long pointer) {
        final long off = pointer & 0x00FFFFFF_FFFFFFFFl;
        return off;
    }

    public String metrics() {
        return metrics.metrics();
    }


    @Data
    class CompactMap2Metrics {
        final long creationTstamp = System.currentTimeMillis();

        final AtomicLong totalCompactCount = new AtomicLong();
        final AtomicLong totalCompactDurationNs = new AtomicLong();
        final AtomicLong lastCompactTimestamp = new AtomicLong();
        final AtomicLong lastCompactDurationNs = new AtomicLong();
        final AtomicLong lastCompactObjectsDeleted = new AtomicLong();
        final AtomicLong lastCompactObjectsSurvived = new AtomicLong();

        final AtomicLong totalGetQueryCount = new AtomicLong();
        final AtomicLong totalGetKeyCount = new AtomicLong();
        final AtomicLong totalPeekCount = new AtomicLong();

        final AtomicLong totalAddCount = new AtomicLong();
        final AtomicLong totalAddSize = new AtomicLong();


        public String json() {
            StringBuilder sb = new StringBuilder();
            sb.append("{\n");
            sb.append("  \"creationTstamp\": ").append(creationTstamp).append(",\n");
            sb.append("  \"totalCompactCount\": ").append(totalCompactCount.get()).append(",\n");
            sb.append("  \"totalCompactDurationNs\": ").append(totalCompactDurationNs.get()).append(",\n");
            sb.append("  \"lastCompactTimestamp\": ").append(lastCompactTimestamp.get()).append(",\n");
            sb.append("  \"lastCompactDurationNs\": ").append(lastCompactDurationNs.get()).append(",\n");
            sb.append("  \"lastCompactObjectsDeleted\": ").append(lastCompactObjectsDeleted.get()).append(",\n");
            sb.append("  \"lastCompactObjectsSurvived\": ").append(lastCompactObjectsSurvived.get()).append(",\n");
            sb.append("  \"totalGetQueryCount\": ").append(totalGetQueryCount.get()).append(",\n");
            sb.append("  \"totalGetKeyCount\": ").append(totalGetKeyCount.get()).append(",\n");
            sb.append("  \"totalPeekCount\": ").append(totalPeekCount.get()).append(",\n");
            sb.append("  \"totalAddCount\": ").append(totalAddCount.get()).append(",\n");
            sb.append("  \"totalAddSize\": ").append(totalAddSize.get()).append("\n");
            sb.append("}");
            return sb.toString();
        }


        public String metrics() {
            // ispiši statistike veličine

            boolean locked = _compactionLock.tryLock();
            if (!locked) return "Can't generate metrics. Compaction in progress";
            @Cleanup("unlock") ReentrantLock unlock = _compactionLock;


            String tstamp = TimeUtils.readableTstamp(creationTstamp);

            long totalUsedSize = 0, totalAllocatedSize = 0;
            for (int idx = 0; idx < CompactMap2.this.segments.length; idx++) {
                ByteList seg = CompactMap2.this.segments[idx];
                if (seg == null) continue;
                totalUsedSize += seg.getUsedSize();
                totalAllocatedSize += seg.getAllocatedSize();
            }

            StringBuilder sb = new StringBuilder();
            sb.append(" Creation date: ").append(tstamp).append("\n");
            sb.append("    Index size: ").append(CompactMap2.this.index.size()).append("\n");
            sb.append("Allocated size: ").append(totalAllocatedSize).append(" bytes\n");
            sb.append("     Used size: ").append(totalUsedSize).append(" bytes\n");
            sb.append(" Segment count: ").append(CompactMap2.this.segments.length).append("\n");

            for (int idx = 0; idx < CompactMap2.this.segments.length; idx++) {
                ByteList seg = CompactMap2.this.segments[idx];
                if (seg == null) {
                    sb.append("          ").append(idx + 1).append(": not in use\n");
                    continue;
                }
                long usedSize = seg.getUsedSize();
                long allocatedSize = seg.getAllocatedSize();
                sb.append("          ").append(idx + 1)
                  .append(": used size: ").append(usedSize)
                  .append(" bytes, allocated size: ").append(allocatedSize)
                  .append(" bytes\n");
            }

            String readableTotalCompactDuration = TimeUtils.toReadable(totalCompactDurationNs.get());
            String avgCompactDuration = TimeUtils.toReadable((long) ((double) totalCompactDurationNs.get() / totalCompactCount.get()));
            String readableLastCompactDuration = TimeUtils.toReadable(lastCompactDurationNs.get());
            String lastCompatTstamp = TimeUtils.readableTstamp(lastCompactTimestamp.get());

            // ispiši statistike korištenja
            sb.append("    add counts: ").append(totalAddCount)
                                         .append(", size: ").append(totalAddSize).append(" bytes\n");
            sb.append("    get counts: query: ").append(totalGetQueryCount.get())
                                                .append(", key: ").append(totalGetKeyCount.get())
                                                .append(", peek: ").append(totalPeekCount.get())
                                                .append("\n");

            sb.append("    compaction: count: ").append(totalCompactCount)
                                                .append(", total duration: ").append(readableTotalCompactDuration)
                                                .append(", avg duration: ").append(avgCompactDuration)
                                                .append("\n");

            sb.append("  last compact: ").append(lastCompatTstamp)
                                         .append(", duration: ").append(readableLastCompactDuration)
                                         .append(", deleted: ").append(lastCompactObjectsDeleted.get())
                                         .append(", survived: ").append(lastCompactObjectsSurvived.get())
                                         .append("\n");


            String res = sb.toString();
            return res;
        }

    }

}
