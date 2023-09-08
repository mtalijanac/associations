package mt.fireworks.associations.cache;

import java.util.Arrays;

public interface SerDes<T> {

    /**
     * Write an object to data array.
     *
     * @return data array with marshalled object
     * @see #unmarshall(byte[])
     */
    byte[] marshall(T val);


    /**
     * Read object T from data array.
     *
     * @param marshalled object
     * @return unmarshalled object
     * @see #marshall(Object)
     */
    T unmarshall(byte[] data);


    /**
     * In place version of unmarshalling provided as more
     * efficient version of unmarshalling interface. <br>
     *
     * Default implementation will drop down to allocating
     * intermediate array and invoke {@code #unmarshall(byte[])}
     *
     * @param data - base cache array
     * @param position - offset in data
     * @param length - len of object
     * @return unmarshalled object
     */
    default T unmarshall(byte[] data, int position, int length) {
        byte[] d = new byte[length];
        System.arraycopy(data, position, d, 0, length);
        return unmarshall(d);
    }


    /**
     * Extract timestamp of object.
     */
    long timestampOfT(T val);


    /**
     * Extract timestamp from marshalled version of object. <br/>
     *
     * Default version is not optimised, and it will
     * firstly unmarshall and than invoke {@code #timestampOfT(Object)}.
     *
     * @param data - marshalled object
     * @return timestamp of object
     */
    default long timestampOfD(byte[] data) {
        T t = unmarshall(data);
        return timestampOfT(t);
    }


    /**
     * In place version of {@code #timestampOfD(byte[])}, used
     * for more efficient reading.
     *
     * @param data - base cache array
     * @param position - offset in data
     * @param length - len of object
     * @return timestamp of object
     */
    default long timestampOfD(byte[] data, int position, int length) {
        T val = unmarshall(data, position, length);
        return timestampOfT(val);
    }


    /**
     * Equals on values. Default version uses {@link Object#equals(Object)}.
     *
     * @param val1
     * @param val2
     * @return true if objects are equals
     */
    default boolean equalsT(T val1, T val2) {
        return val1.equals(val2);
    }

    /**
     * Equals on marshalled verison. Default versions
     * just delegates to {@link Arrays#equals(byte[], byte[]).
     *
     * @param data1 - first marshalled object
     * @param data2 - second marshalled object
     * @return true if objects are equal
     */
    default boolean equalsD(byte[] data1, byte[] data2) {
        return Arrays.equals(data1, data2);
    }

    /**
     * Inplace version of marshalled equals.
     * Default version just checks if marshalled objects
     * are equal.
     *
     * @param data1
     * @param pos1
     * @param len1
     * @param data2
     * @param pos2
     * @param len2
     * @return
     */
    default boolean equalsD(
            byte[] data1, int pos1, int len1,
            byte[] data2, int pos2, int len2
    ) {
        if (len1 != len2) return false;

        for (int idx = 0; idx < len1; idx++)
            if (data1[pos1 + idx] != data2[pos2 + idx])
                return false;

        return true;
    }

}
