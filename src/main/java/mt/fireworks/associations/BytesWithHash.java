package mt.fireworks.associations;

import java.util.Arrays;
import java.util.function.Function;

import org.eclipse.collections.api.block.HashingStrategy;

class BytesWithHash implements HashingStrategy<byte[]> {

    @Override
    public int computeHashCode(byte[] a) {
        if (a == null) return 0;
        int hash = BitsAndBytes.readInt(a, 0);
        return hash;
    }

    @Override
    public boolean equals(byte[] object1, byte[] object2) {
        return Arrays.equals(object1, object2);
    }


    public static byte[] arrayWithHash(byte[] key) {
        if (key == null) return null;

        int hash = 1;
        for (int idx = 0; idx < key.length; idx++) {
            byte element = key[idx];
            hash = 31 * hash + element;
        }

        byte[] keyWithHash = new byte[key.length + 4];
        BitsAndBytes.writeInt(hash, keyWithHash, 0);
        System.arraycopy(key, 0, keyWithHash, 4, key.length);
        return keyWithHash;
    }

    /**
     * Creates delegate keyer that adds a hash to the start of the key.
     * This keyer is compatbile with this hashing strategy.
     *
     * @param keyer - regular key
     * @return delegate keyer with hash prepended
     */
    public static <T> Function<T, byte[]> hashedKeyer(Function<T, byte[]> keyer) {
        return value -> {
            if (value == null) return null;
            byte[] key = keyer.apply(value);
            if (key == null) return null;
            byte[] keyWithHash = BytesWithHash.arrayWithHash(key);
            return keyWithHash;
        };
    }
}
