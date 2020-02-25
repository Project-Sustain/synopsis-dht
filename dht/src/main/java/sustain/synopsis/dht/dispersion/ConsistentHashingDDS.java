package sustain.synopsis.dht.dispersion;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Id mapping scheme that converts a given key to an identifier of the range 0 - 2^128
 */
public class ConsistentHashingDDS implements RingIdMapper {

    // we assume 128-bit identifier.
    public final BigInteger BASE = BigInteger.valueOf(2).pow(128);

    @Override
    public BigInteger getIdentifier(String key) {
        // hash the passed in key. Use it as an 2's complement of a number to construct
        // a positive BigInteger object.
        BigInteger identifier = null;
        try {
            identifier = new BigInteger(1, MessageDigest.getInstance("SHA-1").digest(key.getBytes()));
            identifier = identifier.mod(BASE);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return identifier;
    }
}
