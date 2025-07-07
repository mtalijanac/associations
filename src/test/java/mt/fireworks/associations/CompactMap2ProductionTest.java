package mt.fireworks.associations;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import org.junit.Test;

public class CompactMap2ProductionTest {


    @Test
    public void productionLoadTest() {
    	BytesSerDes serDes = new BytesSerDes();
    	CompactMap2<byte[]> compactMap = new CompactMap2<>(32, 1024*1024, serDes, keyer);

    	// add about 2 milion ~1000 objects
    	// do the compat
    	// ten times:
    	//   add aditional 200 thousands objects, 80% updates of last
    	//   do the compaction
    	// check size¸


        long initialObjects = 2_000_000;
        long additionalObjects = initialObjects / 10;;
        long updatesPercentage = 80;
        long newObjectsPercentage = 100 - updatesPercentage;

        long compactionCount = 10;
        long totalKeyCount = initialObjects + additionalObjects * compactionCount * newObjectsPercentage / 100;

        ThreadLocalRandom random = ThreadLocalRandom.current();

        long totalDataSize = 0;
        for (long i = 0; i < initialObjects; i++) {
        	int dlen = 1000 + random.nextInt(1000);
        	byte[] data = randomData(i, dlen);
        	totalDataSize += data.length;
        	compactMap.add(data);
        }

        System.out.println(compactMap.metrics());

        long lastKey = initialObjects;
        for (int i = 0; i < compactionCount; i++) {
        	for (int j = 0; j < additionalObjects; j++) {
        		int val = random.nextInt(100);
        		long key = val < 20 ? lastKey++
        				            : random.nextLong(lastKey);

        		int dlen = 1000 + random.nextInt(1000);
            	byte[] data = randomData(key, dlen);
            	compactMap.add(data);
        	}

        	compactMap.compact();
        	System.out.println(compactMap.metrics());
        }

        compactMap.compact();
        System.out.println(compactMap.metrics());

    }



    byte[] randomData(long key, int len) {
    	byte[] data = new byte[len];
    	ThreadLocalRandom.current().nextBytes(data);

    	// write key at start of data in low endian format
    	data[0] = (byte) (key & 0xFF);
    	data[1] = (byte) ((key >> 8) & 0xFF);
    	data[2] = (byte) ((key >> 16) & 0xFF);
    	data[3] = (byte) ((key >> 24) & 0xFF);
    	data[4] = (byte) ((key >> 32) & 0xFF);
    	data[5] = (byte) ((key >> 40) & 0xFF);
    	data[6] = (byte) ((key >> 48) & 0xFF);
    	data[7] = (byte) ((key >> 56) & 0xFF);
    	return data;
    }


	static class BytesSerDes implements SerDes<byte[]> {
		@Override
		public byte[] marshall(byte[] val) {
			return val;
		}

		@Override
		public byte[] unmarshall(byte[] data) {
			return data;
		}

		@Override
		public byte[] unmarshall(byte[] data, int position, int length) {
			byte[] res = new byte[length];
			System.arraycopy(data, position, res, 0, length);
			return res;
		}
	}


	Function<byte[], byte[]> keyer = new Function<byte[], byte[]>() {
		public byte[] apply(byte[] t) {
			byte[] key = new byte[8];
			System.arraycopy(t, 0, key, 0, 8);
			return key;
		}
	};


}
