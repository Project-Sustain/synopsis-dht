package sustain.synopsis.ingestion.client.core;

import sustain.synopsis.common.Strand;

import java.util.Set;

public interface StrandPublisher {
    void publish(Set<Strand> strands);
}
