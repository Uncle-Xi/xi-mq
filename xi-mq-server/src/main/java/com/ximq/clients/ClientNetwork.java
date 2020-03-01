package com.ximq.clients;

import com.ximq.common.network.XiDecoder;
import com.ximq.common.network.XiEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.net.InetSocketAddress;

/**
 * @description: ClientNetwork
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class ClientNetwork {

    ClientHandler clientHandler;
    InetSocketAddress address;

    public ClientNetwork(InetSocketAddress address, ClientHandler clientHandler){
        this.address = address;
        this.clientHandler = clientHandler;
    }

    public void connect() {
        new Thread(() -> {
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            try {
                Bootstrap b = new Bootstrap();
                b.group(workerGroup);
                b.channel(NioSocketChannel.class);
                b.option(ChannelOption.SO_KEEPALIVE, true);
                b.handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new LengthFieldBasedFrameDecoder(65535, 0, 2, 0, 2));
                        pipeline.addLast(new XiDecoder());
                        pipeline.addLast(new LengthFieldPrepender(2));
                        pipeline.addLast(new XiEncoder());
                        pipeline.addLast(clientHandler);
                    }
                });
                ChannelFuture f = b.connect(address).sync();
                f.channel().closeFuture().sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                workerGroup.shutdownGracefully();
            }
        }).start();
    }

    public ClientHandler getClientHandler() {
        return clientHandler;
    }

    public InetSocketAddress getAddress() {
        return address;
    }
}
