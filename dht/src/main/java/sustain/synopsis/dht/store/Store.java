package sustain.synopsis.dht.store;

public interface Store {

    public boolean open();

    public boolean close();

    public boolean store(byte[] key, byte[] val);

    public Iterator getIterator();
}
