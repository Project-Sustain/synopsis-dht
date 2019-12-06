package sustain.synopsis.dht.store;

public interface Iterator {
    public boolean hasNext();

    public StoredEntry next();

}
