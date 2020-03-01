package com.ximq.clients.consumer;

import com.alibaba.fastjson.JSONObject;
import com.ximq.common.message.Request;
import com.ximq.common.message.Response;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;


/**
 * @description: ConsumerConnectPool
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class ConsumerConnectPool {

    public static ChannelPoolMap<String, FixedChannelPool> poolMap;
    private static final Bootstrap bootstrap = new Bootstrap();

    public ConsumerConnectPool(InetSocketAddress inetSocketAddress) {
        bootstrap.group(new NioEventLoopGroup());
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.remoteAddress(inetSocketAddress);
        init();
    }

    public void init() {
        poolMap = new AbstractChannelPoolMap<String, FixedChannelPool>() {
            @Override
            protected FixedChannelPool newPool(String key) {
                ChannelPoolHandler handler = new ChannelPoolHandler() {
                    @Override
                    public void channelReleased(Channel ch) throws Exception {
                        ch.writeAndFlush(Unpooled.EMPTY_BUFFER);
                        System.out.println("channelReleased......");
                    }

                    @Override
                    public void channelCreated(Channel ch) throws Exception {
                        ch.pipeline().addLast(new StringEncoder());
                        ch.pipeline().addLast(new DelimiterBasedFrameDecoder(Integer.MAX_VALUE, Delimiters.lineDelimiter()[0]));
                        ch.pipeline().addLast(new StringDecoder());
                        ch.pipeline().addLast(new ConsumerHandler());
                    }

                    @Override
                    public void channelAcquired(Channel ch) throws Exception {
                        System.out.println("channelAcquired......");
                    }
                };
                return new FixedChannelPool(bootstrap, handler, 50);
            }
        };

    }

    public Response send(Request req) throws InterruptedException, ExecutionException {
        final Request request = req;
        final FixedChannelPool pool = poolMap.get(req.getPartition());
        Future<Channel> future = pool.acquire();
        future.addListener(new FutureListener<Channel>() {
            @Override
            public void operationComplete(Future<Channel> future) throws Exception {
                Channel channel = future.getNow();
                channel.writeAndFlush(JSONObject.toJSONString(request));
                System.out.println(channel.id());
                pool.release(channel);
            }
        });
        Future<Channel> defaultFuture = future.await();
        return (Response) defaultFuture.get();
    }
}
