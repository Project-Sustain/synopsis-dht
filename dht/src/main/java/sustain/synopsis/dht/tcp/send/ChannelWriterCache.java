package sustain.synopsis.dht.tcp.send;

import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Thilina Buddhika
 */
public class ChannelWriterCache {

    private static ChannelWriterCache instance;

    private final Logger logger = Logger.getLogger(ChannelWriterCache.class);
    private Map<String, NettyChannelWriter> cache = new ConcurrentHashMap<>();
    private NettyChannelConnector channelConnector;

    private ChannelWriterCache() throws TransportError {
        channelConnector = new NettyChannelConnector();
    }

    public static ChannelWriterCache getInstance() throws TransportError {
        if (instance == null) {
            synchronized (ChannelWriterCache.class) {
                if (instance == null) {
                    instance = new ChannelWriterCache();
                }
            }
        }
        return instance;
    }

    public synchronized NettyChannelWriter getChannelWriter(String endPoint) throws
            TransportError {
        if (cache.containsKey(endPoint)) {
            if (logger.isTraceEnabled()) {
                logger.trace("Cache Hit. Returning the existing connection to " +
                        endPoint);
            }
            return cache.get(endPoint);
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Cache Miss. Creating a new connection to " +
                        endPoint);
            }
            String[] endPointData = endPoint.split(":");
            int port = Integer.parseInt(endPointData[1]);
            NettyChannelWriter channelWriter = channelConnector.addNewConnection(
                    endPointData[0], port);
            cache.put(endPoint, channelWriter);
            return channelWriter;
        }
    }
}
