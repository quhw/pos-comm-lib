package com.chinaums.poscomm;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CodecException;
import org.apache.commons.codec.binary.Hex;

import java.util.List;

class RequestDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in,
                          List<Object> out) throws Exception {
        if (in.readableBytes() < 2) {
            return;
        }

        in.markReaderIndex();
        int length = in.readShort();
        if (length > 10 * 1024) {
            throw new CodecException("报文过长");
        }

        if (length == 0) {
            // 心跳包
            out.add(new MsgPacket());
            return;
        }

        if (in.readableBytes() < length) {
            in.resetReaderIndex();
            return;
        }

        byte[] version = new byte[1];
        in.readBytes(version);
        byte[] tpduDst = new byte[2];
        in.readBytes(tpduDst);
        byte[] tpduSrc = new byte[2];
        in.readBytes(tpduSrc);
        byte[] data = new byte[length - 5];
        in.readBytes(data);

        MsgPacket msg = new MsgPacket(Hex.encodeHexString(version),
                Hex.encodeHexString(tpduDst), Hex.encodeHexString(tpduSrc),
                data);
        out.add(msg);
    }
}
