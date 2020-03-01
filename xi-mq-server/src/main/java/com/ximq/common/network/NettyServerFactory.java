package com.ximq.common.network;

import com.ximq.common.Node;
import com.ximq.common.message.Request;
import com.ximq.common.util.StringUtils;
import com.ximq.server.MQServer;
import com.ximq.server.NetServer;
import com.ximq.server.NetServerFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.net.InetSocketAddress;

/**
 * @description: NettyServerFactory
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class NettyServerFactory extends NetServerFactory {

    ServerBootstrap bootstrap = new ServerBootstrap();
    EventLoopGroup bossGroup = new NioEventLoopGroup();
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    InetSocketAddress localAddress;
    NettyServerFactory factory;
    MQServer ms;

    class NettyServerHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            //System.out.println("NettyServerHandler channelActive...");
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            try {
                NettyServer nettyServer = new NettyServer(ctx, ms, factory);
                Request request = (Request) StringUtils.getObjectByClazz((String) msg, Request.class);
                Node node = request.getConfig().getNode();
                ms.configuration.getNodeMap().put(node.getShortKey(), node);
                nettyServer.setRequest(request);
                synchronized (this) {
                    processMessage(nettyServer);
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            // 删除这个节点的 consumer
            InetSocketAddress isa = (InetSocketAddress) ctx.channel().remoteAddress();
            String deadClient = isa.getHostString() + ":" + isa.getPort();
            System.out.println("exception Caught：[" + cause.getMessage() + "] deadClient : [" + deadClient + "].");
            ms.configuration.addDeadClient(deadClient);
            ctx.close();
        }

        private void processMessage(NettyServer server) {
            server.receiveMessage();
        }
    }

    public NettyServerFactory() {
        this.factory = this;
        bootstrap.group(bossGroup, workerGroup);
        bootstrap.channel(NioServerSocketChannel.class);
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new LengthFieldBasedFrameDecoder(65535, 0, 2, 0, 2));
                pipeline.addLast(new XiDecoder());
                pipeline.addLast(new LengthFieldPrepender(2));
                pipeline.addLast(new XiEncoder());
                pipeline.addLast(new NettyServerHandler());
            }
        });
        bootstrap.option(ChannelOption.SO_BACKLOG, 1024);
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
    }

    @Override
    public synchronized void start() {
        this.localAddress = new InetSocketAddress(configuration.getPort());
        new Thread(() -> {
            try {
                System.out.println("binding to port " + localAddress);
                ChannelFuture channelFuture = bootstrap.bind(localAddress).sync();
                channelFuture.channel().closeFuture().sync();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            }
        }).start();
    }

    @Override
    public void setServer(MQServer server) {
        this.ms = server;
    }
}
