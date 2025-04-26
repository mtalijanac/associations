package mt.fireworks.pauseless;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import lombok.*;

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

    /**
     * Interface for unmarshalling byte arrays into objects of type T.
     *
     * @param <T> The type of object to be unmarshalled.
     */
    public interface Unmarshaller<T> {
        T unmarshall(byte[] objData);
    }

    /**
     * Interface for unmarshalling byte arrays into objects of type T.
     * It adds offset logic to basic <code>Unmarshaller</code>.
     *
     * @param <T> The type of object to be unmarshalled.
     */
    public interface UnmarshallerWithOffset<T> {
        T unmarshall(byte[] objData, int off, int len);
    }




    final TrieNode<T> root = new TrieNode<>(0l);

    /**
     * Interns an object based on its byte array representation.
     *
     * @param objData      The byte array representation of the object.
     * @param unmarshaller The unmarshaller to convert byte array to object.
     * @return The interned object.
     */
    public T intern(byte[] objData, Unmarshaller<T> unmarshaller) {
        return intern(objData, 0, objData.length, (data, off, len) -> unmarshaller.unmarshall(data));
    }


    /**
     * Interns an object based on its byte array representation.
     *
     * @param objData      The byte array representation of the object.
     * @param unmarshaller The unmarshaller to convert byte array to object.
     * @return The interned object.
     */
    public T intern(byte[] objData, UnmarshallerWithOffset<T> unmarshaller) {
        return intern(objData, 0, objData.length, unmarshaller);
    }



    /**
     * Interns an object based on its byte array representation.
     *
     * @param objData      The byte array representation of the object.
     * @param off          The starting offset in the byte array.
     * @param len          The length of the byte array to use.
     * @param unmarshaller The unmarshaller to convert byte array to object.
     * @return The interned object.
     */
    public T intern(byte[] objData, int off, int len, UnmarshallerWithOffset<T> unmarshaller) {
        TrieNode<T> current = root;
        int endIdx = off + len;
        for (int idx = off; idx < endIdx; idx += 8) {
            int keyLen = Math.min(endIdx - idx, 8);
            long nodeKey = BitsAndBytes.bytes2long(objData, idx, keyLen);
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

        final AtomicReference<
                MutableLongObjectMap<
                    AtomicReference<
                        Object>>> childrenRef = new AtomicReference<>();

        final ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();


        public MutableLongObjectMap<AtomicReference<Object>> getChildren() {
            MutableLongObjectMap<AtomicReference<Object>> children = childrenRef.get();
            if (children != null) return children;

            children = new LongObjectHashMap<>();
            boolean compareAndSetRes = childrenRef.compareAndSet(null, children);
            if (compareAndSetRes) return children;

            children = childrenRef.get();
            return children;
        }


        public TrieNode<T> childNode(long nodeKey) {
            MutableLongObjectMap<AtomicReference<Object>> children = getChildren();

            AtomicReference<Object> ref = children.get(nodeKey);
            if (ref != null) {
                TrieNode<T> child = (TrieNode<T>) ref.get();
                return child;
            }

            @Cleanup("unlock") WriteLock lock = rwlock.writeLock();
            lock.lock();
            AtomicReference<Object> childRef = (AtomicReference<Object>)
                children.getIfAbsentPutWithKey(nodeKey, k -> new AtomicReference<>(new TrieNode<>(k)));

            TrieNode<T> res = (TrieNode<T>) childRef.get();
            return res;
        }


        /** Read value stored under nodeKey */
        public T childValue(long nodeKey, UnmarshallerWithOffset<T> supplier, byte[] key, int off, int len) {
            MutableLongObjectMap<AtomicReference<Object>> children = getChildren();

            AtomicReference<Object> ref = children.get(nodeKey);
            if (ref != null) {
                T val = (T) ref.get();
                return val;
            }

            @Cleanup("unlock") WriteLock lock = rwlock.writeLock();
            lock.lock();

            AtomicReference<T> valRef = (AtomicReference<T>) children.getIfAbsentPutWithKey(nodeKey,
                    k -> new AtomicReference(supplier.unmarshall(key, off, len))
            );
            T val = valRef.get();
            return val;
        }


        public T getValue(UnmarshallerWithOffset<T> supplier, byte[] key, int off, int len) {
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
        if (node.childrenRef.get() == null) return;

        mapCount.incrementAndGet();

        MutableLongObjectMap<Object> children = (MutableLongObjectMap<Object>) node.childrenRef.get();

        children.keySet().forEach(each -> {
            Object obj = node.getChildren().get(each);
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
