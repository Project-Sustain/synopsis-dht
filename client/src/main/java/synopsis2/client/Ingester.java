package synopsis2.client;

import synopsis2.SpatioTemporalRecord;

public interface Ingester {
    public void initialize();

    public boolean hasNext();

    public SpatioTemporalRecord next();
}
