package mt.fireworks.timecache;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map.Entry;

import lombok.*;

@AllArgsConstructor
public class CacheEntry<K, T> implements Entry<K, List<T>>{
    @Getter String name;
    @Getter K key;
    @Getter List<T> value;

    public List<T> setValue(List<T> value) {
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
        sb.append("Name: '").append(name).append("', Key '")
          .append(keyName)
          .append("':\n");

        for (T v: value) {
            sb.append("\t").append(v).append("\n");
        }
        return sb.toString();
    }

}
