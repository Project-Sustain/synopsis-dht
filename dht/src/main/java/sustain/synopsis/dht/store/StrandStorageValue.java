package sustain.synopsis.dht.store;

import com.google.protobuf.InvalidProtocolBufferException;
import sustain.synopsis.common.CommonUtil;
import sustain.synopsis.common.ProtoBuffSerializedStrand;
import sustain.synopsis.common.Strand;
import sustain.synopsis.storage.lsmtree.Mergeable;
import sustain.synopsis.storage.lsmtree.StreamSerializable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Wrapper for Strands (serialized with Protocol Buffers) before storing them in the LSMTree. This implementation uses a
 * lazy deserialization to reduce the serialization overhead. If the Strand object is requested through the
 * #get methods, deserialization is performed. This implementation is not thread safe.
 */
public class StrandStorageValue implements Mergeable<StrandStorageValue>, StreamSerializable {

    private List<byte[]> strands = new ArrayList<>();

    public StrandStorageValue(byte[]... serializedStrands) {
        strands.addAll(Arrays.asList(serializedStrands));
    }

    public StrandStorageValue() {
        // for deserialization
    }

    @Override
    public void merge(StrandStorageValue strandStorageValue) {
        this.strands.addAll(strandStorageValue.strands);
    }

    @Override
    public void serialize(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(strands.size());
        for (byte[] strand : this.strands) {
            dataOutputStream.writeInt(strand.length);
            dataOutputStream.write(strand);
        }
    }

    @Override
    public void deserialize(DataInputStream dataInputStream) throws IOException {
        int strandCount = dataInputStream.readInt();
        this.strands = new ArrayList<>(strandCount);
        for (int i = 0; i < strandCount; i++) {
            byte[] serializedStrand = new byte[dataInputStream.readInt()];
            dataInputStream.readFully(serializedStrand);
            this.strands.add(serializedStrand);
        }
    }

    public List<Strand> getStrands() throws InvalidProtocolBufferException {
        return getProtoBuffSerializedStrands().stream().map(CommonUtil::protoBuffToStrand).collect(Collectors.toList());
    }

    public List<ProtoBuffSerializedStrand> getProtoBuffSerializedStrands() throws InvalidProtocolBufferException {
        List<ProtoBuffSerializedStrand> protoBuffSerializedStrands = new ArrayList<>(this.strands.size());
        for (byte[] serializedStrand : strands) {
            protoBuffSerializedStrands.add(ProtoBuffSerializedStrand.newBuilder().mergeFrom(serializedStrand).build());
        }
        return protoBuffSerializedStrands;
    }
}
