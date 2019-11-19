package sustain.synopsis.storage.lsmtree;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class LSMTestValue implements Serializable, Mergeable<LSMTestValue> {
    private byte[] val;

    LSMTestValue() {

    }

    LSMTestValue(byte[] payload) {
        this.val = payload;
    }

    LSMTestValue(int size) {
        this.val = new byte[size];
        new Random().nextBytes(this.val);
    }

    @Override
    public void serialize(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(this.val.length);
        dataOutputStream.write(this.val);
    }

    @Override
    public void deserialize(DataInputStream dataInputStream) throws IOException {
        this.val = new byte[dataInputStream.readInt()];
        dataInputStream.readFully(this.val);
    }

    @Override
    public void merge(LSMTestValue lsmTestValue) {
        this.val = lsmTestValue.val; // simply overwrite the byte[]
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LSMTestValue that = (LSMTestValue) o;
        return Arrays.equals(val, that.val);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(val);
    }
}