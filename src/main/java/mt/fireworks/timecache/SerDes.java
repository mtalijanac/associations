package mt.fireworks.timecache;

import java.util.function.BiFunction;
import java.util.function.Function;

public interface SerDes<T, D> {

    /** marshall Object to data for storage */
    Function<T, D> getMarshaller();

    Function<D, T> getUnmarshaller();

    BiFunction<T, T, Boolean> getEqualsT();

    BiFunction<D, D, Boolean> getEqualsD();

    Function<T, Long> getTimestampOfT();

    Function<D, Long> getTimestampOfD();


}
