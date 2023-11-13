package mt.fireworks.associations.cache;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class BytesCacheBuilder {

    private BytesCacheBuilder() {}


    public static interface AddSerdes<T> {
        /**
         * Set {@code CacheSerDes} underlying this map.
         */
        AddAssociation<T> withSerdes(CacheSerDes<T> serdes);

        AddUnmarshaller<T> usingMarshaller(Function<T, byte[]> marshaller);
    }

    public static interface AddUnmarshaller<T> {
        AddAssociation<T> usingUnmarshaller(Function<byte[], T> unmarshaller);

        // FIXME add timestamp functions
        // FIXME add default timestamp function which assoicates timestamp of entry to value
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
        /** Change default allocation rate. Default is 1 Mbyte. */
        Build<T> allocationSize(int sizeInBytes);

        /** Number of internal maps within index; used to store associations. */
        Build<T> indexMapCount(int val);

        Build<T> historyWindowsCount(int val);

        Build<T> futureWindowCount(int val);

        Build<T> windowTimespanMs(long val);

        Build<T> windowTimespan(long amout, TimeUnit unit);

        Build<T> startTimeMillis(long startTimestamp);

        Build<T> enableMetrics();

        BytesCache<T> build();
    }

    public static interface AddAssociationOrBuild<T> extends AddAssociation<T>, Build<T> {
    }


    static class Builder<T> implements AddSerdes<T>, AddAssociationOrBuild<T>, AddUnmarshaller<T> {
        BytesCacheFactory<T> factory = new BytesCacheFactory<>();

        @Deprecated
        Function<T, byte[]> marshaller;
        @Deprecated
        Function<byte[], T> unmarshaller;


        public AddAssociation<T> withSerdes(CacheSerDes<T> serdes) {
            factory.setSerdes(serdes);
            return this;
        }

        public AddAssociationOrBuild<T> associate(String keyName, Function<T, byte[]> association) {
            factory.addKeyer(keyName, association);
            return this;
        }

        public Build<T> allocationSize(int sizeInBytes) {
            factory.setAllocationSize(sizeInBytes);
            return this;
        }

        public Build<T> historyWindowsCount(int val) {
            factory.setHistoryWindowsCount(val);
            return this;
        }

        public Build<T> futureWindowCount(int val) {
            factory.setFutureWindowCount(val);
            return this;
        }

        public Build<T> windowTimespanMs(long val) {
            factory.setWindowTimespanMs(val);
            return this;
        }

        public Build<T> startTimeMillis(long startTimestamp) {
            factory.setStartTimeMillis(startTimestamp);
            return this;
        }

        public Build<T> windowTimespan(long duration, TimeUnit unit) {
            long span = TimeUnit.MILLISECONDS.convert(duration, unit);
            factory.setWindowTimespanMs(span);
            return this;
        }

        public Build<T> enableMetrics() {
            factory.setMetricsEnabled(true);
            return this;
        }

        public Build<T> enableDuplicateCheck() {
            factory.setCheckForDuplicates(true);
            return this;
        }

        public Build<T> indexMapCount(int val) {
            factory.setIndexMapCount(val);
            return this;
        }

        public BytesCache<T> build() {
            return factory.getInstance();
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

}
