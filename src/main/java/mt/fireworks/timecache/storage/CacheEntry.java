package mt.fireworks.timecache.storage;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map.Entry;

import lombok.*;

@AllArgsConstructor
public class CacheEntry<T> implements Entry<byte[], List<T>>{
    @Getter byte[] key;
    @Getter List<T> value;

    public List<T> setValue(List<T> value) {
        throw new UnsupportedOperationException();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Key '")
          .append(new String(key, StandardCharsets.UTF_8))
          .append("':\n");

        for (T v: value) {
            sb.append("\t").append(v).append("\n");
        }
        return sb.toString();
    }

}
