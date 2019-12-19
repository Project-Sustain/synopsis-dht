package sustain.synopsis.dht.store;

import sustain.synopsis.storage.lsmtree.Serializable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

/**
 * Key used when a LSM tree is stored in the LSM tree.
 * We only use the temporal attributes to construct a key, because the data is stored
 * in the temporal order for a given entity.
 */
public class StrandStorageKey implements Comparable<StrandStorageKey>, Serializable {

    private long startTS;
    private long endTS;

    public StrandStorageKey(long startTS, long endTS) {
        this.startTS = startTS;
        this.endTS = endTS;
    }

    public StrandStorageKey() {
        // for data deserialization
    }

    @Override
    public int compareTo(StrandStorageKey o) {
        return Long.compare(this.startTS, o.startTS);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StrandStorageKey that = (StrandStorageKey) o;
        return startTS == that.startTS && endTS == that.endTS;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTS, endTS);
    }

    @Override
    public void serialize(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeLong(startTS);
        dataOutputStream.writeLong(endTS);
    }

    @Override
    public void deserialize(DataInputStream dataInputStream) throws IOException {
        this.startTS = dataInputStream.readLong();
        this.endTS = dataInputStream.readLong();
    }

    public String toString(){
        return startTS + "_" + endTS;
    }
}
