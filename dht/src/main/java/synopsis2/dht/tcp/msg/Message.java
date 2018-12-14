package synopsis2.dht.tcp.msg;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public interface Message {
    public int getType();

    public void serialize(DataOutputStream dos) throws IOException;

    public void deserialize(DataInputStream dis) throws IOException;
}
