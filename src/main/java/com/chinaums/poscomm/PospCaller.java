package com.chinaums.poscomm;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 8583通讯类，采用netty异步长连接方式。
 * <p>
 * Keeper thread负责周期性维护连接，确保连接可用 Heartbeat thread周期性发送心跳到每个连接 Idle state
 * handler发现没有应答报文的连接并断开 RefreshContextTimer 用来清理contextMap，弹出超时的请求
 *
 * @author 焕文
 */
public class PospCaller implements RemovalListener<String, Context> {
    private static Logger log = LoggerFactory.getLogger(PospCaller.class);
    private static final AttributeKey<Integer> CHANNEL_SEQ_KEY = AttributeKey.valueOf("channel.seq.key");
    private static final AttributeKey<String> CHANNEL_ID_KEY = AttributeKey.valueOf("channel.id.key");

    // 逗号分隔的主机地址
    private String hosts;
    // 连接重连间隔，单位秒
    private int reconnectInterval = 30;
    // 心跳间隔，单位秒
    private int heartBeatInterval = 10;
    // 心跳超时时间，单位秒，0关闭心跳
    private int heartBeatTimeout = 30;
    // 通讯超时时间，单位秒
    private int timeout = 55;
    // 连接超时时间
    private int connectTimeout = 10;
    // 线程池
    private ExecutorService executorService;
    // 每个服务器维持多少个长连接
    private int connectionsPerHost = 4;
    // 是否发送心跳
    private boolean sendHeartBeat = true;

    private NioEventLoopGroup bossGroup;
    private Bootstrap bootStrap;

    /**
     * 连接地址
     */
    private SocketAddress[] addrs;

    /**
     * 连接通道
     */
    private Channel[] channels;

    /**
     * 上下文map，保存调用上下文，用来处理异步应答
     */
    private Cache<String, Context> contextMap;

    private HeartBeat heartBeat;

    private KeeperThread keeper;

    private Timer refreshContextMapTimer;

    private Random random = new Random(System.currentTimeMillis());

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public String getHosts() {
        return hosts;
    }

    public int getReconnectInterval() {
        return reconnectInterval;
    }

    public void setReconnectInterval(int reconnectInterval) {
        this.reconnectInterval = reconnectInterval;
    }

    public int getHeartBeatInterval() {
        return heartBeatInterval;
    }

    public void setHeartBeatInterval(int heartBeatInterval) {
        this.heartBeatInterval = heartBeatInterval;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public int getConnectionsPerHost() {
        return connectionsPerHost;
    }

    public void setConnectionsPerHost(int connectionsPerHost) {
        this.connectionsPerHost = connectionsPerHost;
    }

    public boolean isSendHeartBeat() {
        return sendHeartBeat;
    }

    public void setSendHeartBeat(boolean sendHeartBeat) {
        this.sendHeartBeat = sendHeartBeat;
    }

    public void start() {
        contextMap = CacheBuilder
                .newBuilder()
                .expireAfterWrite(timeout, TimeUnit.SECONDS)
                .removalListener(this)
                .build();

        bossGroup = new NioEventLoopGroup();
        bootStrap = new Bootstrap();
        bootStrap.group(bossGroup);
        bootStrap.channel(NioSocketChannel.class);
        bootStrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootStrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout * 1000);
        bootStrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(
                        new IdleStateHandler(heartBeatTimeout, 0, 0),
                        new ResponseEncoder(), new RequestDecoder(),
                        new ClientHandler());
            }
        });

        addrs = getHostAddresses();
        channels = new Channel[addrs.length];

        keeper = new KeeperThread();
        keeper.start();

        heartBeat = new HeartBeat();
        heartBeat.start();

        refreshContextMapTimer = new Timer();
        refreshContextMapTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    contextMap.cleanUp();
                } catch (Exception e) {
                }
            }
        }, 1000, 1 * 1000);
    }

    public void stop() {
        refreshContextMapTimer.cancel();
        heartBeat.end();
        keeper.end();
        bossGroup.shutdownGracefully().syncUninterruptibly();
    }

    public void send(MsgPacket requestMsg, final PospCallerCallback callback) {
        Channel channel = getChannel();
        if (channel == null) {
            log.warn("没有可用服务，无法发送报文请求");
            executorService.submit(new Runnable() {
                public void run() {
                    callback.onError(new Exception("系统维护中，请稍后再试"));
                }
            });
            return;
        }

        String msgKey;
        Integer seq;
        synchronized (channel) {
            seq = channel.attr(CHANNEL_SEQ_KEY).get();
            if (seq == null) {
                seq = 0;
            }
            int nextSeq = seq + 1;
            if (nextSeq > 9999) {
                nextSeq = 0;
            }
            channel.attr(CHANNEL_SEQ_KEY).set(nextSeq);

            String id = channel.attr(CHANNEL_ID_KEY).get();
            if (id == null) {
                id = UUID.randomUUID().toString();
                channel.attr(CHANNEL_ID_KEY).set(id);
            }

            msgKey = id + ":" + seq;
        }

        String seqStr = new DecimalFormat("0000").format(seq);
        MsgPacket msg = new MsgPacket(requestMsg.getVersion(),
                requestMsg.getTpduDst(), seqStr, requestMsg.getPayload());


        Context context = new Context();
        context.callback = callback;
        context.origTpduSrc = requestMsg.getTpduSrc();
        contextMap.put(msgKey, context);

        log.info("数据发往: {}", channel.remoteAddress());
        channel.writeAndFlush(msg);
    }

    public MsgPacket send(final MsgPacket requestMsg) throws Throwable {
        final CountDownLatch cdl = new CountDownLatch(1);
        final ResultWrapper result = new ResultWrapper();
        send(requestMsg, new PospCallerCallback() {
            public void onSuccess(MsgPacket msg) {
                result.msg = msg;
                cdl.countDown();
            }

            public void onTimeout() {
                result.throwable = new Exception("调用超时");
                cdl.countDown();
            }

            public void onError(Throwable e) {
                result.throwable = e;
                cdl.countDown();
            }
        });

        cdl.await();

        if (result.throwable != null) {
            throw result.throwable;
        }
        return result.msg;
    }

    private class ClientHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object in)
                throws Exception {
            final MsgPacket msg = (MsgPacket) in;
            if (msg.getPayload() == null) {
                log.debug("收到心跳应答: {}", ctx.channel().remoteAddress());
                // 心跳返回，忽略
                return;
            }

            log.info("收到应答数据: {}", ctx.channel().remoteAddress());

            String id = ctx.channel().attr(CHANNEL_ID_KEY).get();
            String msgKey = id + ":" + msg.getTpduDst();

            final Context context = contextMap.asMap().remove(msgKey);
            if (context == null) {
                log.error("无法找到原路由请求: {}", msgKey);
                return;
            }

            executorService.submit(new Runnable() {
                public void run() {
                    MsgPacket result = new MsgPacket(msg.getVersion(),
                            context.origTpduSrc, msg.getTpduSrc(), msg.getPayload());
                    context.callback.onSuccess(result);
                }
            });
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            if (cause instanceof IOException) {
                log.error("通讯异常：{}", cause.getMessage());
            } else {
                log.error("未捕获异常：", cause);
            }

            if (ctx.channel().isActive()) {
                ctx.channel().close();
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
                throws Exception {
            if (evt instanceof IdleStateEvent) {
                log.info("连接空闲超时，强制关闭：{}", ctx.channel().remoteAddress());
                ctx.channel().close();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.warn("服务连接关闭: {}", ctx.channel().remoteAddress());
        }
    }

    private Channel getChannel() {
        // 随机选择一个连接通道，如果该连接通道不可用，则依次选择下一个。
        int start = random.nextInt(channels.length);
        int currentChannel = start;

        while (true) {
            if (channels[currentChannel] != null
                    && channels[currentChannel].isActive()) {
                return channels[currentChannel];
            } else {
                currentChannel = (currentChannel + 1) % channels.length;
                if (currentChannel == start)
                    break;
            }
        }

        return null;
    }

    // 不停的循环检查连接状态，没有连接则自动连接。
    private class KeeperThread extends Thread {
        public volatile boolean stop = false;

        public void end() {
            stop = true;
            this.interrupt();
        }

        public void run() {
            Thread.currentThread().setName(
                    "Keeper " + Thread.currentThread().getId());
            while (!stop) {
                try {
                    for (int i = 0; i < channels.length; i++) {
                        if (!isChannelActive(i)) {
                            try {
                                connect(i);
                            } catch (Exception e) {
                                log.error("连接异常", e);
                            }
                        }
                    }
                    Thread.sleep(reconnectInterval * 1000);
                } catch (Exception e) {
                }
            }
        }
    }

    private void connect(int index) {
        SocketAddress addr = addrs[index];
        try {
            log.info("尝试连接服务: {}", addr);
            // 注意，这里是同步等待，如果网络连接时间过长，会卡住所有线程。
            ChannelFuture f = bootStrap.connect(addr).await();
            if (f.isSuccess()) {
                log.info("成功连接服务: {}", addr);
                channels[index] = f.channel();
            } else {
                log.warn("无法连接服务: {}", addr);
            }
        } catch (Exception e) {
            log.error("异常：" + addr, e);
        }
    }

    private SocketAddress[] getHostAddresses() {
        String[] hostArray = hosts.split(",");
        List<SocketAddress> addrList = new ArrayList<SocketAddress>(
                hostArray.length);
        for (String hostStr : hostArray) {
            String[] hostParts = hostStr.trim().split(":");
            String host = hostParts[0];
            int port = Integer.parseInt(hostParts[1]);
            InetSocketAddress addr = new InetSocketAddress(host, port);

            addrList.add(addr);
        }

        // 复制服务器地址，产生多个长连接
        List<SocketAddress> list = new ArrayList<SocketAddress>(addrList.size() * connectionsPerHost);
        for (int i = 0; i < connectionsPerHost; i++) {
            list.addAll(addrList);
        }

        return list.toArray(new SocketAddress[0]);
    }

    public void onRemoval(RemovalNotification<String, Context> notification) {
        if (notification.wasEvicted()) {
            final Context context = notification.getValue();
            log.info("路由超时删除，msgKey={}", notification.getKey());
            executorService.submit(new Runnable() {
                public void run() {
                    context.callback.onTimeout();
                }
            });
        }
    }

    private boolean isChannelActive(int index) {
        return index >= 0 && index < channels.length && channels[index] != null
                && channels[index].isActive();
    }

    // 周期性向每个连接发送心跳报文，这里不检查应答，应答由idlestatehandler来处理。
    private class HeartBeat extends Thread {
        public volatile boolean stop = false;

        public void end() {
            stop = true;
            this.interrupt();
        }

        public void run() {
            Thread.currentThread().setName(
                    "HeartBeat " + Thread.currentThread().getId());

            try {
                Thread.sleep(heartBeatInterval * 1000);
            } catch (Exception e) {
            }

            while (!stop) {
                try {
                    if (sendHeartBeat) {
                        for (int i = 0; i < channels.length; i++) {
                            try {
                                if (isChannelActive(i)) {
                                    log.debug("发送心跳: {}",
                                            channels[i].remoteAddress());
                                    channels[i].writeAndFlush(new MsgPacket());
                                } else {
                                    log.warn("服务未连接，无法发送心跳: {}", addrs[i]);
                                }
                            } catch (Exception e1) {
                                log.warn("发送心跳异常", e1);
                            }
                        }
                    }

                    Thread.sleep(heartBeatInterval * 1000);
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        }
    }
}
