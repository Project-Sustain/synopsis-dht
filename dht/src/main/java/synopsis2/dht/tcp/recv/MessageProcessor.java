package synopsis2.dht.tcp.recv;

/**
 * @author Thilina Buddhika
 */
public interface MessageProcessor {
    /**
     * Process the message received via a TCP socket
     * @param message serialized message
     */
    public void enqueue(byte[] message);
}
