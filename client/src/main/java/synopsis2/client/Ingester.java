package synopsis2.client;

import sustain.synopsis.common.Strand;

public interface Ingester {
    public void initialize();

    public boolean hasNext();

    public Strand next();
}
