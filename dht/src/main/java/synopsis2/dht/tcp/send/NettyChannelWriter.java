package synopsis2.dht.tcp.send;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/**
 * @author Thilina Buddhika
 */
public class NettyChannelWriter {

    private static final long FLUSHING_THRESHOLD = 1024 * 1024 * 10; // 1MB
    private long counter;
    private Channel channel;

    public NettyChannelWriter(Channel channel) {
        this.channel = channel;
    }

    public synchronized void writeData(byte[] payload, boolean immediately) {
        ChannelFuture future = channel.write(payload);
        channel.flush();
        counter += payload.length;
        if (immediately || counter >= FLUSHING_THRESHOLD) {
            try {
                future.sync();
            } catch (InterruptedException ignore) {
                ignore.printStackTrace();
            }
            counter = 0;
        }
    }
}
