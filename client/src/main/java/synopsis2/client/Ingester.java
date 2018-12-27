package synopsis2.client;

import synopsis2.Strand;

public interface Ingester {
    public void initialize();

    public boolean hasNext();

    public Strand next();
}
