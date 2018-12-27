package synopsis2;

import io.sigpipe.sing.graph.Path;

public class Strand {
    private final String geohash;
    private final long timestamp;
    private final Path path;
    private final String key;

    public Strand(String geohash, long timestamp, Path path, String key) {
        this.geohash = geohash;
        this.timestamp = timestamp;
        this.path = path;
        this.key = key;
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

    public String getKey() {
        return key;
    }

    public void merge(Strand other){
        if(this.path.size() != other.path.size()){
            throw new IllegalArgumentException("Path lengths do not match!");
        }
        if(!this.key.equals(other.key)){
            throw new IllegalArgumentException("Keys do not match. Data container merge failed.");
        }
        this.path.get(path.size()-1).getData().merge(other.path.get(other.path.size()-1).getData());
    }
}
