package sustain.synopsis.ingestion.client.publishing;

import sustain.synopsis.common.Strand;

import java.util.Collection;

public interface StrandPublisher {
    void publish(long messageId, Iterable<Strand> strands);

    default void terminateSession(){

    }

    default long getTotalStrandsPublished() {
        return 0;
    }
}
