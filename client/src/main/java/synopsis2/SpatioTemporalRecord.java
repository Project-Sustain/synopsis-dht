package synopsis2;

import io.sigpipe.sing.dataset.Metadata;
import io.sigpipe.sing.graph.Path;

public class SpatioTemporalRecord {
    private final String geohash;
    private final long timestamp;
    private final Path path;

    public SpatioTemporalRecord(String geohash, long timestamp, Path path) {
        this.geohash = geohash;
        this.timestamp = timestamp;
        this.path = path;
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
