package mt.fireworks.associations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.function.Function;

import lombok.AllArgsConstructor;
import lombok.Setter;
import mt.fireworks.associations.BytesMap.Index;

/**
 * Done using "The Java Fluent API Designer Crash Course"
 *
 * @see <a href="https://blog.jooq.org/the-java-fluent-api-designer-crash-course/">Crash course</a>
 */
class BytesMapBuilder  {

    private BytesMapBuilder() {}


    public static interface AddSerdes<T> {
        /**
         * Set {@code CacheSerDes} underlying this map.
         */
        AddAssociation<T> withSerdes(SerDes<T> serdes);

        AddUnmarshaller<T> usingMarshaller(Function<T, byte[]> marshaller);
    }

    public static interface AddUnmarshaller<T> {
        AddAssociation<T> usingUnmarshaller(Function<byte[], T> unmarshaller);
    }

    public static interface AddAssociation<T> {
        /**
         * Add association function to this map.
         *
         * @param keyName - name of index / association
         * @param association - function used to extract correlation.
         */
        AddAssociationOrBuild<T> associate(String keyName, Function<T, byte[]> association);
    }

    public static interface Build<T> {
        /**
         * Change default allocation rate. Default is 1 Mbyte.
         */
        Build<T> allocationSize(int sizeInBytes);

        BytesMap<T> build();
    }

    public static interface AddAssociationOrBuild<T> extends AddAssociation<T>, Build<T> {
    }


    static class Builder<T> implements AddSerdes<T>, AddAssociationOrBuild<T>, AddUnmarshaller<T> {
        SerDes<T> serdes;
        HashMap<String, Function<T, byte[]>> keyers = new HashMap<>();
        Integer allocationSize;

        Function<T, byte[]> marshaller;
        Function<byte[], T> unmarshaller;


        public AddAssociation<T> withSerdes(SerDes<T> serdes) {
            this.serdes = serdes;
            return this;
        }

        public AddAssociationOrBuild<T> associate(String keyName, Function<T, byte[]> association) {
            keyers.put(keyName, association);
            return this;
        }

        public Build<T> allocationSize(int sizeInBytes) {
            allocationSize = sizeInBytes;
            return this;
        }

        public BytesMap<T> build() {
            ArrayList<Index<T>> indexes = new ArrayList<>(keyers.size());
            for (Entry<String, Function<T, byte[]>> e: keyers.entrySet()) {
                String name = e.getKey();
                Function<T, byte[]> keyer = e.getValue();
                Index<T> index = new Index<>(name, keyer);
                indexes.add(index);
            }

            if (marshaller != null && unmarshaller != null) {
                this.serdes = new EmbeddedSerdes(marshaller, unmarshaller);
            }

            return new BytesMap<>(serdes, indexes, allocationSize);
        }


        public AddAssociation<T> usingUnmarshaller(Function<byte[], T> unmarshaller) {
            this.unmarshaller = unmarshaller;
            return this;
        }

        public AddUnmarshaller<T> usingMarshaller(Function<T, byte[]> marshaller) {
            this.marshaller = marshaller;
            return this;
        }
    }


    @AllArgsConstructor
    static class EmbeddedSerdes<T> implements SerDes<T> {
        @Setter Function<T, byte[]> marshaller;
        @Setter Function<byte[], T> unmarshaller;

        public byte[] marshall(T val) {
            return marshaller.apply(val);
        }

        public T unmarshall(byte[] data) {
            return unmarshaller.apply(data);
        }
    }

}
