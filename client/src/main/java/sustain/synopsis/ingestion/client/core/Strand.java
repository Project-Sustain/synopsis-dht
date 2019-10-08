package sustain.synopsis.ingestion.client.core;

import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.graph.Path;
import sustain.synopsis.sketch.graph.Vertex;
import sustain.synopsis.sketch.serialization.SerializationException;
import sustain.synopsis.sketch.serialization.SerializationInputStream;
import sustain.synopsis.sketch.serialization.SerializationOutputStream;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class Strand {
    private final String geohash;
    /**
     * Upper temporal boundary as provided by the {@link TemporalQuantizer}
     */
    private final long toTimestamp;

    /**
     * Lower temporal boundary for the strand.
     */
    private final long fromTimeStamp;

    /**
     * Feature values
     */
    private final Path path;

    /**
     * Auto-constructed key used for comparing multiple strands and for debugging purposes
     */
    private final String key;

    /**
     * Metadata pertaining to the current strand
     */
    private final Properties metadata;

    public Strand(String geohash, long fromTimeStamp, long toTimestamp, Path path) {
        this.geohash = geohash;
        this.toTimestamp = toTimestamp;
        this.fromTimeStamp = fromTimeStamp;
        this.path = path;
        this.metadata = new Properties();
        this.key = generateKey(geohash, fromTimeStamp, toTimestamp, path);
    }

    public Strand(SerializationInputStream sis) throws IOException, SerializationException {
        this.geohash = sis.readUTF();
        this.fromTimeStamp = sis.readLong();
        this.toTimestamp = sis.readLong();
        int pathSize = sis.readInt();
        this.path = new Path();
        for (int i = 0; i < pathSize; i++) {
            Vertex v = new Vertex(sis);
            path.add(v);
        }
        int metadataSize = sis.readInt();
        this.metadata = new Properties();
        for (int i = 0; i < metadataSize; i++) {
            metadata.setProperty(sis.readUTF(), sis.readUTF());
        }
        this.key = generateKey(this.geohash, this.fromTimeStamp, this.toTimestamp, this.path);
    }

    public void merge(Strand other) {
        if (this.path.size() != other.path.size()) {
            throw new IllegalArgumentException("Path lengths do not match!");
        }
        if (!this.key.equals(other.key)) {
            throw new IllegalArgumentException("Keys do not match. Data container merge failed.");
        }
        this.path.get(path.size() - 1).getData().merge(other.path.get(other.path.size() - 1).getData());
    }

    public void addMetadata(String key, String val) {
        metadata.setProperty(key, val);
    }

    public void serialize(SerializationOutputStream dos) throws IOException {
        dos.writeUTF(geohash);
        dos.writeLong(fromTimeStamp);
        dos.writeLong(toTimestamp);
        dos.writeInt(path.size());
        for (Vertex v : path) {
            v.serialize(dos);
        }
        dos.writeInt(metadata.size());
        for (String key : metadata.stringPropertyNames()) {
            dos.writeUTF(key);
            dos.writeUTF(metadata.getProperty(key));
        }
    }

    public String getGeohash() {
        return geohash;
    }

    public long getFromTimeStamp(){
        return fromTimeStamp;
    }

    public long getToTimestamp() {
        return toTimestamp;
    }

    public Path getPath() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Strand strand = (Strand) o;
        return Objects.equals(key, strand.key) && Objects.equals(this.metadata, strand.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, metadata);
    }

    private static String generateKey(String geohash, long fromTimeStamp, long toTimestamp, Path path) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(geohash).append(",").append(toTimestamp).append(",").append(fromTimeStamp);
        for (Vertex v : path) {
            Feature feature = v.getLabel();
            stringBuilder.append(",").append(feature.getName()).append("=").append(feature.dataToString());
        }
        return stringBuilder.toString();
    }
}
