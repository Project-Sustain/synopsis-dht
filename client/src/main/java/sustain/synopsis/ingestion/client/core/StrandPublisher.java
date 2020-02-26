package sustain.synopsis.ingestion.client.core;

import sustain.synopsis.common.Strand;

import java.util.Collection;

public interface StrandPublisher {
    void publish(Collection<Strand> strands);
}
