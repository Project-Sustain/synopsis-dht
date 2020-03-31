package sustain.synopsis.dht.store;

public class SessionValidator {

    public SessionValidationResponse validate(String dataseId, long sessionId) {
        // TODO: we need to fix this once the metadata server is ready with another gRPC service
        SessionValidationResponse response =
                new SessionValidationResponse(true, "test_user", System.currentTimeMillis());
        return response;
    }

    // todo: we need to replace this with appropriate message type from the session validation gRPC service of the
    // metadata server
    public static class SessionValidationResponse {
        public boolean valid;
        // optional
        public String userId;
        public long sessionStartTS;

        public SessionValidationResponse(boolean valid, String userId, long sessionStartTS) {
            this.valid = valid;
            this.userId = userId;
            this.sessionStartTS = sessionStartTS;
        }
    }
}
