package mt.fireworks.associations;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.api.block.procedure.primitive.ObjectLongProcedure;

public class CompactMapTest {

    
    public static void main(String[] args) {
        SerDes<String> serDes = Associations.stringSerDes(StandardCharsets.UTF_8);
        
        Function<String, byte[]> keyer = new Function<String, byte[]>() {
            @Override
            public byte[] apply(String val) {
                if (val == null) return null;
                String sub = StringUtils.substring(val, 0, 8);
                byte[] key = sub.getBytes(StandardCharsets.US_ASCII);
                return key;
            }
        };
        
        
        CompactMap<String> map = new CompactMap<>(3, serDes, keyer);
        
        String marko = "marko";
        map.add(marko);
        map.add(marko);
        
        map.add("markovo");
        
        map.add("12345678");
        map.add("123456789");
        map.add("12345678A");
        printEntries(map);
        
        map.tick();
        printEntries(map);
        
        map.forEach(new Procedure<String>() {
            public void value(String each) {
                System.out.println(each);
            }
        });
    }
    
    static void printEntries(CompactMap<String> map) {
        map.index.forEachKeyValue(new ObjectLongProcedure<byte[]>() {
            public void value(byte[] key, long parameter) {
                String keyCh = new String(key, StandardCharsets.US_ASCII);
                String livePointer = Long.toHexString(parameter);
                System.out.println(keyCh + " -> " + livePointer );
            }
        });
        
        System.out.println();
    }
}
