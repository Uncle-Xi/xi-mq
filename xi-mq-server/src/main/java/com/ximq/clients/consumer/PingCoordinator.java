package com.ximq.clients.consumer;

import com.ximq.clients.ClientNetwork;
import com.ximq.clients.RecordFuture;
import com.ximq.common.XiMQThread;
import com.ximq.common.message.Request;
import com.ximq.common.message.Response;

import java.net.InetSocketAddress;

/**
 * @description: PingCoordinater
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class PingCoordinator extends XiMQThread {

    private Consumer consumer;
    private ConsumerHandler handler;
    private ClientNetwork network;

    public PingCoordinator(Consumer consumer) {
        super("PingCoordinator");
        //String coordinator = consumer.getConfig().getCoordinator();
        //String[] ba = coordinator.split(":");
        //InetSocketAddress isa = new InetSocketAddress(ba[1], Integer.valueOf(ba[2]));
        //this.handler = new ConsumerHandler();
        //this.network = new ClientNetwork(isa, handler);
        //this.network.connect();
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(5 * 60 * 1000);
                Request request = new Request();
                this.handler.send(request, new RecordFuture() {
                    @Override
                    public void callback(Response response) {
                        // TODO update 连接信息
                        //if (consumer.connnectChanged()){
                        //    consumer.updatedConnect();
                        //}
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
