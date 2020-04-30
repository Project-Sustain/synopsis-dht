package sustain.synopsis.dht.services.query;

import sustain.synopsis.dht.store.services.TargetQueryRequest;

import java.util.concurrent.CompletableFuture;

public interface QueryProcessor {
    CompletableFuture<Boolean> process(TargetQueryRequest queryRequest,
                                        QueryResponseHandler responseHandler);
}
