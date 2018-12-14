package synopsis2.dht.tcp.send;

/**
 * @author Thilina Buddhika
 */
public class TransportError extends Exception {

    public TransportError(String message, Throwable cause) {
        super(message, cause);
    }
}
