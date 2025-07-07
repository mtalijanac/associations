
package mt.fireworks.associations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import lombok.EqualsAndHashCode;

public class CompactMap2Test {


    @Test
    public void testAddAngGetUsingKey() {
    	TestSerDes serDes = new TestSerDes();
    	CompactMap2<TestObject> compactMap = new CompactMap2<>(serDes, TestObject::getIdBytes);

    	// Add test object
        TestObject obj1 = new TestObject("1", "value1");
        byte[] key1 = compactMap.add(obj1);

        // Get by object query
        TestObject retrieved1 = compactMap.get(key1);
        assertNotNull("Should retrieve object by key", retrieved1);
        assertEquals("Should have correct id", obj1.id, retrieved1.id);
        assertEquals("Should have correct value", obj1.value, retrieved1.value);


        // Verify non-existent object returns null
        byte[] key2 = Arrays.copyOf(key1, key1.length);
        key2[0]++;
        TestObject notFound = compactMap.get(key2);
        assertNull("Should return null for non-existent key", notFound);
    }


    @Test
    public void testAddAndGetUsingQuery() {
    	TestSerDes serDes = new TestSerDes();
    	CompactMap2<TestObject> compactMap = new CompactMap2<>(serDes, TestObject::getIdBytes);

        // Add test object
        TestObject obj1 = new TestObject("1", "value1");
        compactMap.add(obj1);

        // Get by object query
        TestObject retrieved1 = compactMap.get(new TestObject("1", null));
        assertNotNull("Should retrieve object by query", retrieved1);
        assertEquals("Should have correct id", "1", retrieved1.id);
        assertEquals("Should have correct value", "value1", retrieved1.value);


        // Verify non-existent object returns null
        TestObject notFound = compactMap.get(new TestObject("99", null));
        assertNull("Should return null for non-existent key", notFound);
    }


    @Test
    public void testCompaction() {
    	TestSerDes serDes = new TestSerDes();
    	CompactMap2<TestObject> compactMap = new CompactMap2<>(serDes, TestObject::getIdBytes);

        // Add multiple objects
        List<TestObject> objects = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            TestObject obj = new TestObject(String.valueOf(i), "value" + i);
            objects.add(obj);
            compactMap.add(obj);
        }

        // Trigger compaction
        compactMap.compact();

        // Verify all objects are still accessible
        for (TestObject obj : objects) {
            TestObject retrieved = compactMap.get(obj.getIdBytes());
            assertNotNull("Object should be accessible after compaction: " + obj.id, retrieved);
            assertEquals("Object should maintain correct value after compaction", obj.value, retrieved.value);
        }
    }


    // about 116 seconds on my machine
    @Test
    public void test20x() throws InterruptedException {
		for (int i = 0; i < 20; i++) {
			testConcurrentAccess();
		}
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException {
    	TestSerDes serDes = new TestSerDes();
    	CompactMap2<TestObject> compactMap = new CompactMap2<>(32, 4 * 1024 * 1024, serDes, TestObject::getIdBytes);

        int threadCount = 20;
        int objectsPerThread = 100_000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCounter = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < objectsPerThread; i++) {
                        String id = threadId + "-" + i;
                        TestObject obj = new TestObject(id, "value-" + id);
                        compactMap.add(obj);

                        TestObject retrieved = compactMap.get(obj);
                        if (retrieved == null) {
                        	System.err.println("Object should be retrievable after add");
                        	System.exit(-1);
                        }

                        assertNotNull("Object should be retrievable after add", retrieved);

                        if (!retrieved.equals(obj)) {
                        	System.err.println("Object should be equal to added");
                        	System.exit(-1);
                        }

                        assertEquals(obj, retrieved);
                        successCounter.incrementAndGet();

                        // Occasionally trigger compaction
                        if (i % 20_000 == 0) {
                            compactMap.tick();
                        }
                    }
                }
                finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        executor.awaitTermination(1, java.util.concurrent.TimeUnit.MINUTES);

        assertEquals("Map size and count added should be equal", threadCount * objectsPerThread, compactMap.size());
        assertEquals("All operations should succeed", threadCount * objectsPerThread, successCounter.get());
    }



    @EqualsAndHashCode
    static class TestObject {
        final String id;
        final String value;

        TestObject(String id, String value) {
            this.id = id;
            this.value = value;
        }

        byte[] getIdBytes() {
            return id.getBytes();
        }

    }

    static class TestSerDes implements SerDes<TestObject> {
        @Override
        public byte[] marshall(TestObject val) {
            byte[] idBytes = val.id.getBytes();
            byte[] valueBytes = val.value != null ? val.value.getBytes() : new byte[0];

            ByteBuffer buffer = ByteBuffer.allocate(4 + idBytes.length + 4 + valueBytes.length);
            buffer.putInt(idBytes.length);
            buffer.put(idBytes);
            buffer.putInt(valueBytes.length);
            buffer.put(valueBytes);

            return buffer.array();
        }

        @Override
        public TestObject unmarshall(byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data);

            int idLength = buffer.getInt();
            byte[] idBytes = new byte[idLength];
            buffer.get(idBytes);
            String id = new String(idBytes);

            int valueLength = buffer.getInt();
            byte[] valueBytes = new byte[valueLength];
            buffer.get(valueBytes);
            String value = new String(valueBytes);

            return new TestObject(id, value);
        }

        @Override
        public TestObject unmarshall(byte[] data, int position, int length) {
            byte[] slice = new byte[length];
            System.arraycopy(data, position, slice, 0, length);
            return unmarshall(slice);
        }
    }
}
