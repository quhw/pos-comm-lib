package com.chinaums.poscomm;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ResponseEncoder extends MessageToByteEncoder<MsgPacket> {
    private static Logger log = LoggerFactory.getLogger(ResponseEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, MsgPacket in, ByteBuf out)
            throws Exception {
        try {
            if (in.getPayload() == null) {
                // 心跳包
                out.writeShort(0);
                return;
            }

            out.writeShort(5 + in.getPayload().length);
            out.writeBytes(Hex.decodeHex(in.getVersion().toCharArray()));
            out.writeBytes(Hex.decodeHex(in.getTpduDst().toCharArray()));
            out.writeBytes(Hex.decodeHex(in.getTpduSrc().toCharArray()));
            out.writeBytes(in.getPayload());
        } catch (Exception e) {
            log.error("Encode error", e);
        }
    }

}
