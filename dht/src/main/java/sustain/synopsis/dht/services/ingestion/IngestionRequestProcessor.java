package sustain.synopsis.dht.services.ingestion;

import sustain.synopsis.dht.store.services.IngestionRequest;
import sustain.synopsis.dht.store.services.IngestionResponse;
import sustain.synopsis.dht.store.services.TerminateSessionRequest;
import sustain.synopsis.dht.store.services.TerminateSessionResponse;

import java.util.concurrent.CompletableFuture;

public interface IngestionRequestProcessor {
    CompletableFuture<IngestionResponse> process(IngestionRequest ingestionRequest);

    CompletableFuture<TerminateSessionResponse> process(TerminateSessionRequest terminateSessionRequest);
}
