package mt.fireworks.pauseless;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class PauselessObjectPool<T> {
    
    final AtomicInteger index = new AtomicInteger(0);
    
    final Class<T> requiredType;
    final int capacity;
    final ConcurrentLinkedQueue<T> q;
    
    final Supplier<T> constructor;
    final Consumer<T> clearObject;
    
    final AtomicLong borrowCounter = new AtomicLong();
    final AtomicLong returnCounter = new AtomicLong();
    final AtomicLong newCounter = new AtomicLong();
    final AtomicLong cachedCounter = new AtomicLong();
    
    public PauselessObjectPool(
        Class<T> requiredType,
        int capacity,
        final Supplier<T> constructor,
        final Consumer<T> clearObject
    ) {
        this.requiredType = requiredType;
        this.capacity = capacity;
        this.constructor = constructor;
        this.clearObject = clearObject;
        this.q = new ConcurrentLinkedQueue<>();
        this.count = new AtomicLong(0);
    }
    
    AtomicLong count;
    volatile long min = Long.MAX_VALUE;
    
    public void prepopulate() {
        for (int i = 0; i < capacity; i++) {
            T obj = constructor.get();
            q.offer(obj);
        }
        this.count = new AtomicLong(capacity);
    }
    
    public T borrowObject() {
        borrowCounter.incrementAndGet();
        
        T obj = q.poll();
        if (obj != null) {
            long size = count.decrementAndGet();
            if (size < min) min = size;
            cachedCounter.incrementAndGet();
            
            return obj;
        }
        newCounter.incrementAndGet();
        return constructor.get();
    }
    
    public void returnObject(T obj) {
        returnCounter.incrementAndGet();
        
        if (clearObject != null) clearObject.accept(obj);
        boolean offer = q.offer(obj);
        if (offer) count.incrementAndGet();
    }
    
    @Override
    public String toString() {
        return
            "Q of: " + requiredType.getSimpleName() +
            ", borrows: " + borrowCounter.get() +
            ", new: " + newCounter.get() +
            ", cached: " + cachedCounter.get() +
            ", returns: " + returnCounter.get()
            + ", q size: " + q.size()
            + ", min: " + min
            ;
    }
}
