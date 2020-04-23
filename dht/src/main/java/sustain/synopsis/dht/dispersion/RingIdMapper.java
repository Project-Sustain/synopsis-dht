package sustain.synopsis.dht.dispersion;

import java.math.BigInteger;

/**
 * Maps keys into the ring identifier space
 */
public interface RingIdMapper {
    /**
     * Map the given key into to a ring id
     *
     * @param key Key in the form of a string
     * @return Identifier in ring identifier
     */
    BigInteger getIdentifier(String key);
}
