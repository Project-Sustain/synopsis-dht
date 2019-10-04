package synopsis2.dht.store;

public interface Iterator {
    public boolean hasNext();

    public StoredEntry next();

}
