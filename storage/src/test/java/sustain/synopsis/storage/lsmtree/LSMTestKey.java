package sustain.synopsis.storage.lsmtree;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

public class LSMTestKey implements Serializable, Comparable<LSMTestKey> {

    private int key;

    LSMTestKey() {
    }

    LSMTestKey(int k) {
        this.key = k;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LSMTestKey that = (LSMTestKey) o;
        return key == that.key;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public int compareTo(LSMTestKey o) {
        return Integer.compare(this.key, o.key);
    }

    @Override
    public void serialize(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(this.key);
    }

    @Override
    public void deserialize(DataInputStream dataInputStream) throws IOException {
        this.key = dataInputStream.readInt();
    }

    @Override
    public String toString() {
        return "LSMTestKey{" + "key=" + key + '}';
    }
}
