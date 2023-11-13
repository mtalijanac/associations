package mt.fireworks.associations;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.eclipse.collections.api.block.HashingStrategy;

import lombok.AllArgsConstructor;

public class Associations {

    private Associations() {}


    @AllArgsConstructor
    public static class StringSerDes implements SerDes<String> {
        Charset charset;

        public byte[] marshall(String val) {
            return val.getBytes();
        }

        public String unmarshall(byte[] data) {
            return new String(data, charset);
        }

        public String unmarshall(byte[] data, int position, int length) {
            return new String(data, position, length, charset);
        }

    }

    public static SerDes<String> stringSerDes() {
        return stringSerDes(StandardCharsets.UTF_8);
    }

    public static SerDes<String> stringSerDes(Charset charset) {
        return new StringSerDes(charset);
    }


    public static HashingStrategy<byte[]> bytesHashingStrategy() {
        return new BytesHashingStrategy();
    }



    public static class BytesSerDes implements SerDes<byte[]> {
        public byte[] marshall(byte[] val) {
            return val;
        }

        public byte[] unmarshall(byte[] data) {
            return data;
        }
    }


    public static SerDes<byte[]> bytesSerDes() {
        return new BytesSerDes();
    }
}
