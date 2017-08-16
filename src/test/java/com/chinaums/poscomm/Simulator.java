package com.chinaums.poscomm;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty服务框架  *  * @author 焕文  *
 */
public class Simulator {
    private static Logger log = LoggerFactory.getLogger(Simulator.class);
    static private EventLoopGroup bossGroup = new NioEventLoopGroup();
    static private EventLoopGroup workerGroup = new NioEventLoopGroup();

    private int port = 1234;
    private String name = "com.chinaums.poscomm.Simulator";

    private ChannelHandler[] createHandlers() {
        return new ChannelHandler[]{
                new ResponseEncoder(), new RequestDecoder(),
                new ServerHandler()
        };
    }

    public void start() throws Exception {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelHandler[] handlers = createHandlers();
                for (ChannelHandler handler : handlers) {
                    ch.pipeline().addLast(handler);
                }
            }
        }).option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.SO_REUSEADDR, true);
        ChannelFuture cf = b.bind(port).await();
        if (!cf.isSuccess()) {
            log.error("无法绑定端口：" + port);
            throw new Exception("无法绑定端口：" + port);
        }
        log.info("服务[{}]启动完毕，监听端口[{}]", name, port);
    }

    public void stop() {
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        log.info("服务[{}]关闭。", name);
    }

    private class ServerHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object in) throws Exception {
            MsgPacket msg = (MsgPacket) in;
            MsgPacket out = new MsgPacket(msg.getVersion(), msg.getTpduSrc(), msg.getTpduDst(), msg.getPayload());
            ctx.channel().writeAndFlush(out);
        }
    }

    public static void main(String[] args) throws Exception {
        new Simulator().start();
    }
}