package sustain.synopsis.ingestion.client.publishing;

import sustain.synopsis.common.Strand;

import java.util.Collection;

public interface StrandPublisher {
    void publish(Iterable<Strand> strands);

    default void terminateSession(){};
}
