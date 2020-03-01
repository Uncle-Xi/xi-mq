package com.ximq.clients;

import com.ximq.common.message.Request;
import com.ximq.common.message.Response;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * @description: ClientHandler
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public abstract class ClientHandler extends ChannelInboundHandlerAdapter {

    protected abstract Response getPartition(Request request);

    public abstract void send(Request request, RecordFuture recordFuture);

    public abstract void subscribe(Request request, RecordFuture recordFuture);
}
