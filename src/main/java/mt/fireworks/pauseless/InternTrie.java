package mt.fireworks.pauseless;

import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Map/Trie structure used for object intern during object deserialization.
 * This structure has better performance than {@link String#intern()}
 * while simultaneously avoiding GC overhead as only objects seen
 * for first time are deseriliazied. <br>
 *
 * While deserializing objects it is usually quite normal to:
 *  - create vast number of small objects within short timespan,
 *    putting pressure on GC
 *  - significant portion of objects are recurring values
 *
 * In case of String objects, there is {@code String#intern()} method
 * used to add/get canonical String representation. However to invoke
 * this method one needs a string object, thus deserialization and
 * allocation penalty is allready present: <br>
 *
 *    byte[] data = .... // example array containing string data
 *    String myString = new String(data).intern(); // first create, than intern
 *
 *
 * InternTrie is structure created for this usecase:<br>
 *
 *   InternTrie<String> myInternTrie = new InternTrie();
 *   byte[] data = .... // example array containing string data
 *   String myString = myInternTrie.intern(data, (obj, off, len) -> new String(obj, off, len));
 *
 * InternTrie stores object in internal map/true structure and uses
 * data as internal key for instantiated object. Thus objects which are
 * recurring are instantiated only once. <br>
 *
 * Primary value of using InternTrie is visible on very short objects.
 * Strings like "1" or "true" or "Y" are very common. The longer the
 * object data is, the less chance for object being recurring
 * and the longer lookup takes. <br>
 *
 * Short strings (less than 8 bytes) are instantiated about twice as slow
 * as just instancing string with new. This is still much faster than
 * instancing string and than invoking intern operation. The longer data key
 * is, more fetches within trie are needed thus performance starts to fall.
 * At about data keys beyond ~50 bytes performance is on par with new/intern
 * combo. However this is only true if allocation rate is considered, as
 * InternTrie will not consume any additional memory for occurring objects
 * and thus lowers avoids GC enforced bottlenecks as repeating objects
 * are deserialized only once. <br>
 *
 * Additional benefit is using InternTrie as temporary buffer.
 * Assume deserialization which happens only sporadically, during which large
 * number of recurring objects are deserialized and instantiated.
 * By instantiating InternTrie at start of deserialization, the one can
 * deseriliaze repeating objects only once, and than remove InternTrie from
 * scope as job is done leaving to GC to collect all interned values. <br>
 *
 * Yet another benefit of ItnerTrie is an ability to intern any object type.
 * As byte data is used as key of object, actual type/content of object in
 * Java form is irrelevant. Thus it is possible to use InternTrie also
 * as intern for other types like Long and Integer, or any other pojo etc..
 */
public class InternTrie<T> {

    public interface Unmarshaller<T> {
        T unmarshall(byte[] objData, int off, int len);
    }

    final TrieNode<T> root = new TrieNode<T>(0l);

    public T intern(byte[] objData, Unmarshaller<T> unmarshaller) {
        return intern(objData, 0, objData.length, unmarshaller);
    }

    public T intern(byte[] objData, int off, int len, Unmarshaller<T> unmarshaller) {
        TrieNode<T> current = root;
        for (int idx = off; idx < off + len; idx += 8) {
            long nodeKey = BitsAndBytes.bytes2long(objData, idx, 8);
            int keyLen = Math.min(objData.length - idx, 8);
            if (keyLen < 8) {
                return current.childValue(nodeKey, unmarshaller, objData, off, len);
            }

            current = current.childNode(nodeKey);
        }

        T value = current.getValue(unmarshaller, objData, off, len);
        return value;
    }


    @RequiredArgsConstructor
    static class TrieNode<T> {

        @NonNull long nodeKey;

        T value;
        MutableLongObjectMap<Object> children;


        public synchronized MutableLongObjectMap<Object> getChildren() {
            if (children == null) {
                children = new LongObjectHashMap<>();
            }

            return children;
        }

        public synchronized TrieNode<T> childNode(long nodeKey) {
            TrieNode<T> child = (TrieNode<T>) getChildren().getIfAbsentPutWithKey(nodeKey, k -> new TrieNode<>(k));
            return child;
        }

        public synchronized T childValue(long nodeKey, Unmarshaller<T> supplier, byte[] key, int off, int len) {
            T val = (T) getChildren().getIfAbsentPutWithKey(nodeKey,
                    k -> supplier.unmarshall(key, off, len)
            );
            return val;
        }

        public synchronized T getValue(Unmarshaller<T> supplier, byte[] key, int off, int len) {
            if (this.value != null) {
                return this.value;
            }

            if (len == 0) {
                return null;
            }

            this.value = supplier.unmarshall(key, off, len);
            return this.value;
        }
    }



    @Override
    public String toString() {
        AtomicInteger nodeCount = new AtomicInteger();
        AtomicInteger mapCount = new AtomicInteger();
        AtomicInteger valueCount = new AtomicInteger();
        about(root, nodeCount, mapCount, valueCount);

        String s = "nodes: " + nodeCount.get() + ", maps: " + mapCount.get() + ", values: " + valueCount.get();
        return s;
    }

    void about(TrieNode node, AtomicInteger nodeCount, AtomicInteger mapCount, AtomicInteger valueCount) {
        nodeCount.incrementAndGet();
        if (node.value != null) valueCount.incrementAndGet();
        if (node.children == null) return;

        mapCount.incrementAndGet();

        node.children.keySet().forEach(each -> {
            Object obj = node.children.get(each);
            if (obj instanceof TrieNode) {
                TrieNode<T> t = (TrieNode<T>) obj;
                about(t, nodeCount, mapCount, valueCount);
            }
            else {
                valueCount.incrementAndGet();
            }
        });
    }

}
