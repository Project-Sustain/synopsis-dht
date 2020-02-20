package sustain.synopsis.dht.store;

import sustain.synopsis.common.Strand;
import sustain.synopsis.sketch.serialization.SerializationException;
import sustain.synopsis.sketch.serialization.SerializationInputStream;
import sustain.synopsis.sketch.serialization.SerializationOutputStream;
import sustain.synopsis.storage.lsmtree.Mergeable;
import sustain.synopsis.storage.lsmtree.Serializable;

import java.io.*;

/**
 * Wrapper for Strands before storing them in the LSMTree
 * This implementation uses a lazy deserialization to reduce the serialization overhead.
 * If the Strand object is requested through the #getMethod(), deserialization is performed.
 * This implementation is not thread safe.
 */
public class StrandStorageValue implements Mergeable<StrandStorageValue>, Serializable {

    private Strand strand;
    private byte[] serializedStrand;

    public StrandStorageValue(byte[] serializedStrand) {
        this.serializedStrand = serializedStrand;
    }

    public StrandStorageValue() {
        // for deserialization
    }

    @Override
    public void merge(StrandStorageValue strandStorageValue) {
        this.getStrand().merge(strandStorageValue.getStrand());
    }

    @Override
    public void serialize(DataOutputStream dataOutputStream) throws IOException {
        byte[] serialized;
        if (strand == null) { // strand is not deserialized yet. Use the serialized data
            serialized = this.serializedStrand;
        } else { // strand may have got merged with another strand - therefore serialize the object
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); SerializationOutputStream sos =
                    new SerializationOutputStream(baos);) {
                strand.serialize(sos);
                sos.flush();
                baos.flush();
                serialized = baos.toByteArray();
            }
        }
        dataOutputStream.writeInt(serialized.length);
        dataOutputStream.write(serialized);
    }

    @Override
    public void deserialize(DataInputStream dataInputStream) throws IOException {
        this.serializedStrand = new byte[dataInputStream.readInt()];
        dataInputStream.readFully(this.serializedStrand);
    }

    public Strand getStrand() {
        if (strand == null) {   // lazy deserialization
            try (ByteArrayInputStream bais = new ByteArrayInputStream(serializedStrand); SerializationInputStream sis = new SerializationInputStream(bais)) {
                Strand strand = new Strand(sis);
                this.strand = strand;
            } catch (IOException | SerializationException e) {
                e.printStackTrace();
            }
        }
        return strand;
    }
}
