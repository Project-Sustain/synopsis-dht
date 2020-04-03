package sustain.synopsis.dht.store;

import com.google.protobuf.InvalidProtocolBufferException;
import sustain.synopsis.common.ProtoBuffSerializedStrand;
import sustain.synopsis.common.Strand;
import sustain.synopsis.common.StrandSerializationUtil;
import sustain.synopsis.storage.lsmtree.MergeError;
import sustain.synopsis.storage.lsmtree.Mergeable;
import sustain.synopsis.storage.lsmtree.StreamSerializable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Wrapper for Strands (serialized with Protocol Buffers) before storing them in the LSMTree. This implementation uses a
 * lazy deserialization to reduce the serialization overhead. If the Strand object is requested through the
 * #getMethod(), deserialization is performed. This implementation is not thread safe.
 */
public class StrandStorageValue implements Mergeable<StrandStorageValue>, StreamSerializable {

    private byte[] serializedStrand;

    public StrandStorageValue(byte[] serializedStrand) {
        this.serializedStrand = serializedStrand;
    }

    public StrandStorageValue() {
        // for deserialization
    }

    @Override
    public void merge(StrandStorageValue strandStorageValue) throws MergeError {
        try {
            this.getStrand().merge(strandStorageValue.getStrand());
        } catch (InvalidProtocolBufferException e) {
            throw new MergeError(e.getMessage(), e);
        }
    }

    @Override
    public void serialize(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(serializedStrand.length);
        dataOutputStream.write(serializedStrand);
    }

    @Override
    public void deserialize(DataInputStream dataInputStream) throws IOException {
        this.serializedStrand = new byte[dataInputStream.readInt()];
        dataInputStream.readFully(this.serializedStrand);
    }

    public Strand getStrand() throws InvalidProtocolBufferException {
        ProtoBuffSerializedStrand strand = ProtoBuffSerializedStrand.newBuilder().mergeFrom(serializedStrand).build();
        return StrandSerializationUtil.fromProtoBuff(strand);
    }
}
