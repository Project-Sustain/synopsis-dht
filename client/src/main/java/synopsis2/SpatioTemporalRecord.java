package synopsis2;

import io.sigpipe.sing.dataset.Metadata;
import io.sigpipe.sing.graph.Path;

public class SpatioTemporalRecord {
    private final String geohash;
    private final long timestamp;
    private final Path path;
    private final Metadata metadata;

    public SpatioTemporalRecord(String geohash, long timestamp, Path path, Metadata metadata) {
        this.geohash = geohash;
        this.timestamp = timestamp;
        this.path = path;
        this.metadata = metadata;
    }

    public String getGeohash() {
        return geohash;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Path getPath() {
        return path;
    }
}
