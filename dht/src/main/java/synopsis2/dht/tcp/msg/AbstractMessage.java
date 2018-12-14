package synopsis2.dht.tcp.msg;

import synopsis2.dht.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public abstract class AbstractMessage implements Message {
    private int type;
    private String sourceAddr;

    public AbstractMessage(int type) {
        this.type = type;
        this.sourceAddr = Util.getNodeAddress();
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public void serialize(DataOutputStream dataOutStr) throws IOException {
        // write metadata
        dataOutStr.writeInt(this.type);
        dataOutStr.writeUTF(this.sourceAddr);
        // write message specific values
        serializeCustomFields(dataOutStr);
        dataOutStr.flush();
    }

    public abstract void serializeCustomFields(DataOutputStream dataOutput) throws IOException;

    @Override
    public void deserialize(DataInputStream dis) throws IOException {
        // we have already read the type
        // read metadata
        this.sourceAddr = dis.readUTF();
        // read message specific fields
        deserializeCustomFields(dis);
    }

    public abstract void deserializeCustomFields(DataInputStream dataInput) throws IOException;

    public String getSourceAddr() {
        return sourceAddr;
    }
}
