package synopsis2.dht.tcp.recv;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * @author Thilina Buddhika
 */
public class ServerHandler extends SimpleChannelInboundHandler<byte[]> {

    private MessageProcessor processor;

    public ServerHandler(MessageProcessor messageProcessor) {
        this.processor = messageProcessor;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, byte[] bytes) throws Exception {
        processor.enqueue(bytes);
    }
}
