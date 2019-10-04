package synopsis2.dht;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import synopsis2.common.kafka.BlockingQueueBackedMessageRelay;
import synopsis2.common.kafka.Consumer;
import synopsis2.common.kafka.MessageRelay;
import synopsis2.dht.tcp.recv.MessageReceiver;
import synopsis2.dht.tcp.recv.ProtocolFactory;
import synopsis2.dht.tcp.recv.ServerHandler;
import synopsis2.dht.zk.ZKError;
import synopsis2.dht.zk.ZooKeeperAgent;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author Thilina Buddhika
 */
public class Node {

    private final Logger logger = Logger.getLogger(Node.class);
    private final int port;

    public Node(int port) {
        this.port = port;
    }

    public void start() {
        EventLoopGroup ctrlBossGroup = new NioEventLoopGroup(1);
        EventLoopGroup ctrlWorkerGroup = new NioEventLoopGroup(1);

        ServerBootstrap ctrlBootstrap = new ServerBootstrap();
        ctrlBootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        ctrlBootstrap.group(ctrlBossGroup, ctrlWorkerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new MessageReceiver(),
                                new ServerHandler(ProtocolFactory.getInstance()));
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
        logger.info("Trying to bind to port: " + this.port);
        try {
            ChannelFuture future = ctrlBootstrap.bind(this.port).sync();
            logger.info("Node is running on " + this.port);
            // register in ZK
            try {
                registerInZK();
            } catch (ZKError zkError) {
                logger.error("Error when registering with ZK. Shutting down.");
                future.channel().close();
                return;
            }

            Thread.sleep(5 * 1000);
            // start the Ring updater thread.
            Ring ring = new Ring();
            new Thread(ring).start();
            Context ctx = Context.getInstance();
            ctx.setRing(ring);

            // start the kafka consumers
            Properties consumerProps = new Properties();
            consumerProps.put("bootstrap.servers", ctx.getProperty(ServerConstants.Configuration.KAFKA_BOOTSTRAP_BROKERS));
            consumerProps.put("group.id", ctx.getProperty(ServerConstants.Configuration.STRAND_INGESTION_CONSUMER_GROUP_ID));
            consumerProps.put("key.deserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("value.deserializer",
                    "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            // timeout values
            consumerProps.put("session.timeout.ms", ServerConstants.TIMEOUTS.KAFKA_CONSUMER_SESSION_TIMEOUT);
            consumerProps.put("heartbeat.interval.ms", ServerConstants.TIMEOUTS.KAFKA_CONSUMER_HEARTBEAT_INTERVAL_MS);
            consumerProps.put("max.poll.records", 5000);

            MessageRelay<String, byte[]> relay = new BlockingQueueBackedMessageRelay<>(100000,
                    ServerConstants.TIMEOUTS.RELAY_READ_TIMEOUT_S,
                    ServerConstants.TIMEOUTS.RELAY_WRITE_TIMEOUT_S,
                    TimeUnit.SECONDS);
            Consumer<String, byte[]> consumer = new Consumer<>(consumerProps, ctx.getProperty(ServerConstants.Configuration.STRAND_INGESTION_TOPIC_PREFIX), relay);
            new Thread(consumer).start();
            logger.info("Started the Kafka consumer.");
            // todo: we need another thread handling messages received from the relay - storage

            future.channel().closeFuture().sync();
            logger.info("Shutting down the node.");
        } catch (InterruptedException e) {
            logger.error("Error starting the node. ", e);
        } finally {
            ctrlBossGroup.shutdownGracefully();
            ctrlWorkerGroup.shutdownGracefully();
        }
    }

    private void registerInZK() throws ZKError {
        String nodeAddress = Util.getNodeAddress();
        ZooKeeper zk = ZooKeeperAgent.getInstance().getZooKeeperInstance();
        // create the group membership directory as well as stream directory
        try {
            try {
                if (zk.exists(ServerConstants.ZK_NODES_ROOT, false) == null) {
                    String groupDir = Util.createZKDirectory(
                            zk, ServerConstants.ZK_NODES_ROOT, null, CreateMode.PERSISTENT);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Created Root ZNode: " + groupDir);
                    }
                }
            } catch (KeeperException e) {
                // ignore. It is likely that the directory is already created by some other node.
            }
            String nodeType = Context.getInstance().getProperty(ServerConstants.Configuration.NODE_TYPE);
            logger.info("Node Type: " + nodeType);
            int virtualNodeCount = Util.getVirtualNodeCount();
            for (int i = 0; i < virtualNodeCount; i++) {
                String individualDir = ServerConstants.ZK_NODES_ROOT + "/" +
                        nodeAddress + ":" + i + ":" + nodeType;
                try {
                    Util.createZKDirectory(zk, individualDir, null, CreateMode.EPHEMERAL);
                } catch (KeeperException e) {
                    logger.error(e.getMessage(), e);
                    throw new ZKError(e.getMessage(), e);
                }
            }

        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
            throw new ZKError(e.getMessage(), e);
        }
    }
}
