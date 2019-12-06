package sustain.synopsis.dht.dispersion;

import sustain.synopsis.dht.Util;
import sustain.synopsis.dht.Ring;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Thilina Buddhika
 */
public class ConsistentHashingDDS implements DataDispersionScheme {
    @Override
    public List<Ring.Entity> processNewMembers(List<String> newNodes) {
        List<Ring.Entity> entities = new ArrayList<>();
        for (String newNode : newNodes) {
            String[] segments = newNode.split(":");
            BigInteger identifier = Util.getIdentifier(segments[0] + ":" + segments[1] + ":" + segments[2]);
            entities.add(new Ring.Entity(identifier, segments[0] + ":" + segments[1],
                    Integer.parseInt(segments[2])));
        }
        return entities;
    }

    @Override
    public BigInteger getIdentifier(String key) {
        return Util.getIdentifier(key);
    }

    @Override
    public boolean incrementalUpdatesToMembers() {
        return true;
    }
}
