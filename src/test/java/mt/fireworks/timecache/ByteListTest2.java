package mt.fireworks.timecache;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.map.mutable.primitive.LongIntHashMap;
import org.junit.Assert;

import lombok.AllArgsConstructor;
import lombok.Data;
import mt.fireworks.timecache.ByteList.ForEachAction;
import mt.fireworks.timecache.ByteList.Peeker;

public class ByteListTest2 {

     @Data @AllArgsConstructor
     static class TData {
         long tstamp;
         String trxData;
     }

    static class TDataSerDes implements SerDes<TData> {

        @Override
        public byte[] marshall(TData val) {
            ByteBuffer bb = ByteBuffer.allocate(1000);
            bb.clear();
            bb.putLong(val.tstamp);

            bb.putShort((short) val.trxData.length());
            bb.put(val.trxData.getBytes(UTF_8));

            bb.flip();
            byte[] data = new byte[bb.limit()];
            bb.get(data);
            return data;
        }

        @Override
        public TData unmarshall(byte[] data) {
            return unmarshall(data, 0, data.length);
        }

        @Override
        public TData unmarshall(byte[] data, int position, int length) {
            ByteBuffer bb = ByteBuffer.wrap(data, position, length);
            long tstamp = bb.getLong();
            short datLen = bb.getShort();
            String strData = new String(data, bb.position(), datLen, StandardCharsets.UTF_8);
            TData res = new TData(tstamp, strData);
            return res;
        }

        @Override
        public long timestampOfT(TData val) {
            return val.tstamp;
        }

    }


    public static void main(String[] args) {
        ArrayList<String> texts = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String trxData = RandomStringUtils.randomAlphanumeric(280, 320);
            texts.add(trxData);
        }

        LongIntHashMap key2TextIdx = new LongIntHashMap();
        TDataSerDes serDes = new TDataSerDes();
        ByteList bl = new ByteList();

        int N = 1_000_000;
        for (int i = 0; i < N; i++) {
            int strIdx = ThreadLocalRandom.current().nextInt(texts.size());
            String text = texts.get(strIdx);
            TData tData = new TData(System.currentTimeMillis(), text);
            byte[] val = serDes.marshall(tData);
            long key = bl.add(val);
            key2TextIdx.put(key, strIdx);
            byte[] cpy = bl.get(key);
            Assert.assertArrayEquals(val, cpy);
        }

        MutableLongList keyList = LongLists.mutable.ofAll(key2TextIdx.keySet());

        for (int i = 0; i < 10*N; i++) {
            int rndIdx = ThreadLocalRandom.current().nextInt(keyList.size());
            long key = keyList.get(rndIdx);
            int txtIdx = key2TextIdx.get(key);
            String srcTxt = texts.get(txtIdx);

            byte[] keyData = bl.get(key);
            TData cpyObject = serDes.unmarshall(keyData);

            String cpyTxt = cpyObject.getTrxData();
            Assert.assertEquals(srcTxt, cpyTxt);
        }
        System.out.println("Random access pass");

        keyList.forEach(new LongProcedure() {
            public void value(long key) {
                byte[] keyData = bl.get(key);
                TData cpyObject = serDes.unmarshall(keyData);

                int txtIdx = key2TextIdx.get(key);
                String srcTxt = texts.get(txtIdx);

                String cpyTxt = cpyObject.getTrxData();
                Assert.assertEquals(srcTxt, cpyTxt);
            }
        });

        System.out.println("Key walk pass");


        bl.forEach(new Peeker<ByteList.ForEachAction>() {
            public ForEachAction peek(long key, byte[] bucket, int pos, int len) {
                TData tData = serDes.unmarshall(bucket, pos, len);
                return ForEachAction.CONTINUE;
            }
        }
        );

        System.out.println("It's ok");
    }
}
