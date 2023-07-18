package mt.fireworks.timecache;

public interface SerDes2<T> {

    byte[] marshall(T val);

    T unmarshall(byte[] data, int position, int length);

    long timestampOfT(T val);

    long timestampOfD(byte[] data, int position, int length);

    boolean equalsT(T val1, T val2);

    boolean equalsD(byte[] data1, byte[] data2);

}
