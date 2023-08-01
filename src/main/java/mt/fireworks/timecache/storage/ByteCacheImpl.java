package mt.fireworks.timecache.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;

import lombok.AllArgsConstructor;
import mt.fireworks.timecache.Cache;
import mt.fireworks.timecache.SerDes2;
import mt.fireworks.timecache.index.Index;
import mt.fireworks.timecache.storage.ByteList.ForEachAction;
import mt.fireworks.timecache.storage.StorageLongKey.Window;

@AllArgsConstructor
public class ByteCacheImpl<T> implements Cache<T, byte[], byte[]>{

    StorageLongKey storage;
    Index<T>[] indexes;
    SerDes2<T> serdes2;

    @Override
    public boolean add(T val) {
        long tstamp = serdes2.timestampOfT(val);
        byte[] data = serdes2.marshall(val);

        long storageIdx = storage.addEntry(tstamp, data);
        if (storageIdx == 0) {
            return false;
        }

        for (Index<T> i: indexes) {
            i.put(val, storageIdx);
        }

        // TODO provjeri da li već postoji ova vrijednost u kešu
        //			za svaki index dohvati listu s istim timestmapom
        //          dohvati sve vrijednosti s istim tstampom
        //          provrti equalsD

        return true;
    }


    @Override
    public Object[] getArray(T val) {
        Object[] result = new Object[indexes.length];
        MutableLongList keysForRemoval = LongLists.mutable.empty();

        for (int idx = 0; idx < indexes.length; idx++) {
            Index<T> index = indexes[idx];
            MutableLongSet strKeys = index.get(val);
            ArrayList<T> ts = new ArrayList<>(strKeys.size());
            result[idx] = ts;

            strKeys.forEach(strKey -> {
                T res = storage.getEntry2(strKey, serdes2);
                if (res != null) ts.add(res);
            });


            if (keysForRemoval != null && keysForRemoval.size() > 0) {
                strKeys.removeAll(keysForRemoval);
                keysForRemoval.clear();
            }
        }

        return result;
    }

    @Override
    public Map<byte[], Collection<T>> getMap(T val) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void tick() {
        // add new and, remove obsolete window from storage
        Window removedWindow = storage.moveWindows();

        // clean indexes
        long endTstamp = removedWindow.endTstamp;
        removedWindow.store.forEach((objPos, bucket, pos, len) -> {
            T obj = serdes2.unmarshall(bucket, pos, len);
            for(Index<T> idx: indexes) {
                idx.clearKey(obj, endTstamp);
            }
            return ForEachAction.CONTINUE;
        });
    }

    @Override
    public String toString() {
        if (serdes2 instanceof MetricSerDes2) {
            return ((MetricSerDes2) serdes2).resetMetrics();
        }
        return "";
    }

}
