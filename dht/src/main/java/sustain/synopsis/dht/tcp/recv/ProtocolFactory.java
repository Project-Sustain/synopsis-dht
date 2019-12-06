package sustain.synopsis.dht.tcp.recv;

import org.apache.log4j.Logger;
import sustain.synopsis.dht.WorkerPool;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class ProtocolFactory implements MessageProcessor {

    private final static ProtocolFactory instance = new ProtocolFactory();
    private final Logger logger = Logger.getLogger(ProtocolFactory.class);
    private final WorkerPool workerPool;

    private ProtocolFactory() {
        this.workerPool = WorkerPool.getInstance();
    }

    public static ProtocolFactory getInstance() {
        return instance;
    }

    public void enqueue(byte[] message) {
        Runnable task = parse(message);
        if (task != null) {
            workerPool.submit(task);
        }
    }

    private Runnable parse(byte[] message) {
        ByteArrayInputStream bais = new ByteArrayInputStream(message);
        DataInputStream dis = new DataInputStream(bais);
        Runnable task = null;
        try {
            int type = dis.readInt();
            switch (type) {

                default:
                    logger.warn("Unsupported message type. Code: " + type);
            }
        } catch (IOException e) {
            logger.error("Error reading from the input stream for control message.", e);
        } finally {
            try {
                bais.close();
                dis.close();
            } catch (IOException ignore) {

            }
        }
        return task;
    }
}

