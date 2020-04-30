package sustain.synopsis.dht.services.query;

import sustain.synopsis.dht.store.services.TargetQueryResponse;

/**
 * Used for handling the query responses ({@link TargetQueryResponse}) returned by reader tasks.
 */
public interface QueryResponseHandler {
    void handleResponse(TargetQueryResponse targetQueryResponse);

    default void handleError(Throwable t) {
    }
}
