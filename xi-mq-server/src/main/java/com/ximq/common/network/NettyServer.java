package com.ximq.common.network;

import com.ximq.common.message.Record;
import com.ximq.common.message.Request;
import com.ximq.common.util.StringUtils;
import com.ximq.server.MQServer;
import com.ximq.server.NetServer;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;

/**
 * @description: NettyServer
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class NettyServer extends NetServer {

    private MQServer server;
    private ChannelHandlerContext ctx;
    private NettyServerFactory factory;

    public NettyServer(ChannelHandlerContext ctx, MQServer server, NettyServerFactory factory) {
        this.ctx = ctx;
        this.server = server;
        this.factory = factory;
    }

    @Override
    public void receiveMessage() {
        try {
            server.processConnectRequest(this);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void sendResponse(Record record) throws IOException {
        ctx.writeAndFlush(StringUtils.getString(record));
    }
}
