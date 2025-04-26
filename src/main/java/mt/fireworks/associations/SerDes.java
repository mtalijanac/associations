package mt.fireworks.associations;

/**
 * T.B.D.
 *
 * @param <T>
 */
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
     * In place unmarshaller. Used as more efficient version of
     * {@code #unmarshall(byte[]).<br>
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
    
    
    default SerDes<T> withMetric() {
        return new MetricSerDes<>(this);
    }

}