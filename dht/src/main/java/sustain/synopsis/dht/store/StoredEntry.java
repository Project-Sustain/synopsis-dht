package sustain.synopsis.dht.store;

public class StoredEntry {
    private byte[] key;
    private byte[] val;

    public StoredEntry(byte[] key, byte[] val) {
        this.key = key;
        this.val = val;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getVal() {
        return val;
    }
}
