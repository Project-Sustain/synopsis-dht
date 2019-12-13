package sustain.synopsis.dht.store;

import sustain.synopsis.common.Strand;
import sustain.synopsis.sketch.serialization.SerializationException;
import sustain.synopsis.sketch.serialization.SerializationInputStream;
import sustain.synopsis.sketch.serialization.SerializationOutputStream;
import sustain.synopsis.storage.lsmtree.Mergeable;
import sustain.synopsis.storage.lsmtree.Serializable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Wrapper for Strands before storing them in the LSMTree
 */
public class StrandStorageValue implements Mergeable<StrandStorageValue>, Serializable {

    private Strand strand;

    public StrandStorageValue(Strand strand) {
        this.strand = strand;
    }

    public StrandStorageValue() {
        // for deserialization
    }

    @Override
    public void merge(StrandStorageValue strandStorageValue) {
        this.strand.merge(strandStorageValue.strand);
    }

    @Override
    public void serialize(DataOutputStream dataOutputStream) throws IOException {
        SerializationOutputStream sos = new SerializationOutputStream(dataOutputStream);
        strand.serialize(sos);
        sos.flush();
        sos.close();
    }

    @Override
    public void deserialize(DataInputStream dataInputStream) throws IOException {
        SerializationInputStream sis = new SerializationInputStream(dataInputStream);
        try {
            this.strand = new Strand(sis);
        } catch (SerializationException e) {
            throw new IOException(e);
        } finally {
            sis.close();
        }
    }

    public Strand getStrand() {
        return strand;
    }
}
