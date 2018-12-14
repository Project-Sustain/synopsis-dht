package synopsis2.dht.tcp.recv;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class MessageReceiver extends ByteToMessageDecoder {
    public MessageReceiver() {
    }

    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf in, List<Object> out) throws Exception {
        if(in.readableBytes() >= 4) {
            in.markReaderIndex();
            int dataLength = in.readInt();
            if(in.readableBytes() < dataLength) {
                in.resetReaderIndex();
            } else {
                byte[] decoded = new byte[dataLength];
                in.readBytes(decoded);
                out.add(decoded);
            }
        }
    }
}
