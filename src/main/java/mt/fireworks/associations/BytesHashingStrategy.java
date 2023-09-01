package mt.fireworks.associations;

import java.util.Arrays;

import org.eclipse.collections.api.block.HashingStrategy;

class BytesHashingStrategy implements HashingStrategy<byte[]> {

    @Override
    public int computeHashCode(byte[] a) {
        if (a == null) return 0;

        int result = 1;
        for (int idx = 0; idx < a.length; idx++) {
            byte element = a[idx];
            result = 31 * result + element;
        }

        return result;
    }

    @Override
    public boolean equals(byte[] object1, byte[] object2) {
        return Arrays.equals(object1, object2);
    }

}
