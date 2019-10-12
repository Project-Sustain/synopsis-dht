package sustain.synopsis.ingestion.client.core;

import java.util.Set;

public interface StrandPublisher {
    void publish(Set<Strand> strands);
}
