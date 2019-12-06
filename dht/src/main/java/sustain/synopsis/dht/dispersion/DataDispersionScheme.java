package sustain.synopsis.dht.dispersion;

import sustain.synopsis.dht.Ring;

import java.math.BigInteger;
import java.util.List;

/**
 * @author Thilina Buddhika
 */
public interface DataDispersionScheme {
    public List<Ring.Entity> processNewMembers(List<String> newNodes);

    public BigInteger getIdentifier(String key);

    public boolean incrementalUpdatesToMembers();
}
