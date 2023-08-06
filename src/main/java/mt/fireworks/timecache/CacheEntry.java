package mt.fireworks.timecache;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map.Entry;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class CacheEntry<K, T> implements Entry<K, T>{
    @Getter String name;
    @Getter K key;
    @Getter T value;

    @Override
    public T setValue(T value) {
        throw new UnsupportedOperationException();
    }


    public String toString() {
        String keyName = null;
        if (key instanceof byte[]) {
            keyName = new String((byte[])key, StandardCharsets.UTF_8);
        }
        else {
            keyName = key.toString();
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Name: '").append(name)
          .append("', Key '").append(keyName).append("'");

        if (value instanceof Collection) {
            sb.append(", values: :\n");
            for (Object o: (Collection<?>) value) {
                sb.append("\t").append(o).append("\n");
            }
        }
        else {
            sb.append(". value:'").append(value).append("'");
        }

        return sb.toString();
    }


}
