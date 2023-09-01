package mt.fireworks.associations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.function.Function;

import mt.fireworks.associations.AssociationMap.SerDes;
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
         * Set {@code SerDes} underlying this map.
         */
        AddAssociation<T> withSerdes(SerDes<T> serdes);
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



    static class Builder<T> implements AddSerdes<T>, AddAssociationOrBuild<T> {
        SerDes<T> serdes;
        HashMap<String, Function<T, byte[]>> keyers = new HashMap<>();
        Integer allocationSize;

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

            return new BytesMap<>(serdes, indexes, allocationSize);
        }
    }

}
