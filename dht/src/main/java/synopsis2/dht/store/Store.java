package synopsis2.dht.store;

public interface Store {

    public boolean open();

    public boolean close();

    public boolean store(byte[] key, byte[] val);

    public Iterator getIterator();
}
