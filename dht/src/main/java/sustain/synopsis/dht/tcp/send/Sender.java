package sustain.synopsis.dht.tcp.send;

import org.apache.log4j.Logger;
import sustain.synopsis.dht.tcp.msg.Message;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class Sender {
    private static Logger logger = Logger.getLogger(Sender.class);

    public static void sendControlMessage(String endpoint, Message message)
            throws TransportError {
        sendControlMessage(endpoint, message, true);
    }

    public static void sendControlMessage(String endpoint, Message message, boolean immediately)
            throws TransportError {
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutStr = new DataOutputStream(new BufferedOutputStream(baOutputStream));
        try {
            message.serialize(dataOutStr);
            dataOutStr.flush();
            baOutputStream.flush();
            byte[] serializedData = baOutputStream.toByteArray();
            sendBytes(endpoint, serializedData, immediately);
        } catch (IOException e) {
            String errMsg = "Error sending control message to " + endpoint;
            logger.error(errMsg, e);
            throw new TransportError(errMsg, e);
        } finally {
            try {
                baOutputStream.close();
                dataOutStr.close();
            } catch (IOException e) {
                logger.error("Error closing streams.", e);
            }
        }
    }

    public static void sendBytes(String endpoint, byte[] bytes) throws TransportError {
        sendBytes(endpoint, bytes, true);
    }

    private static void sendBytes(String endpoint, byte[] bytes, boolean immediately) throws TransportError {
        ChannelWriterCache.getInstance().getChannelWriter(endpoint).writeData(bytes, immediately);
    }
}
