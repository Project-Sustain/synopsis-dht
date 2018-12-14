package synopsis2.dht.tcp.send;


import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;

/**
 * @author Thilina Buddhika
 */
public class NettyChannelConnector {

    private final Logger logger = Logger.getLogger(NettyChannelConnector.class);
    private Bootstrap bootstrap;

    public NettyChannelConnector() {
        EventLoopGroup group = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new DataLengthEncoder());
    }

    public NettyChannelWriter addNewConnection(String serverHost, int serverPort) throws TransportError {
        try {
            // Make a new connection.
            ChannelFuture f = bootstrap.connect(serverHost, serverPort).sync();
            // generate payload and send
            Channel channel = f.channel();
            return new NettyChannelWriter(channel);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
            throw new TransportError(e.getMessage(), e);
        }
    }
}

