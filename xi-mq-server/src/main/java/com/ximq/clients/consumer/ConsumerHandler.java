package com.ximq.clients.consumer;

import com.ximq.clients.ClientHandler;
import com.ximq.clients.RecordFuture;
import com.ximq.common.Node;
import com.ximq.common.XiMQThread;
import com.ximq.common.config.ClientConfig;
import com.ximq.common.message.Request;
import com.ximq.common.message.Response;
import com.ximq.common.util.StringUtils;
import io.netty.channel.ChannelHandlerContext;

import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @description: consumer handler
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class ConsumerHandler extends ClientHandler {

    private final LinkedBlockingQueue<Request> sendQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<Response> resultQueue = new LinkedBlockingQueue<>();
    private ChannelHandlerContext ctx;

    public ConsumerHandler() {
        try {
            SubThread subThread = new SubThread(this);
            subThread.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Response getPartition(Request request) {
        try {
            if (sendMsg(request)) {
                return resultQueue.take();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        //System.out.println("channelActive ... ");
    }

    private boolean sendMsg(Request msg) throws InterruptedException {
        try {
            while (ctx == null) {
                Thread.sleep(200);
            }
            Node node = new Node((InetSocketAddress) ctx.channel().localAddress(), msg.getTopic(), msg.getGroupId());
            node.setType(Node.Type.CONSUMER);
            String key = node.toString();
            msg.setKey(key);
            ClientConfig config = msg.getConfig();
            if (config == null) {
                config = new ClientConfig();
            }
            config.setNode(node);
            config.setInstance(node.getShortKey());
            msg.setConfig(config);
            ctx.channel().writeAndFlush(StringUtils.getString(msg));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext channelHandlerContext, Object object) throws Exception {
        try {
            resultQueue.add((Response) StringUtils.getObjectByClazz((String) object, Response.class));
        } catch (Exception e) {
            System.out.println("转换错误");
            e.printStackTrace();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.out.println("与服务器断开连接:" + cause.getMessage());
        ctx.close();
    }

    @Override
    public void send(Request request, RecordFuture recordFuture) {
        throw new UnsupportedOperationException("不支持操作！");
    }

    @Override
    public void subscribe(Request request, RecordFuture recordFuture) {
        try {
            if (sendQueue.add(request)) {
                Response response = resultQueue.poll(5, TimeUnit.SECONDS);
                if (response == null || response.getData() == null) { return; }
                recordFuture.callback(response);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class SubThread extends XiMQThread {

        ClientHandler handler;

        public SubThread(ClientHandler handler) {
            super("SubThread...");
            this.handler = handler;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Request request = sendQueue.take();
                    sendMsg(request);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
