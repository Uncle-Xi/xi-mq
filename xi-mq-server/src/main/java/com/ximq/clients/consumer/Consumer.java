package com.ximq.clients.consumer;

import com.ximq.clients.ClientNetwork;
import com.ximq.clients.Clients;
import com.ximq.clients.ConnectStringParser;
import com.ximq.clients.RecordFuture;
import com.ximq.clients.producer.ProducerHandler;
import com.ximq.common.OpCode;
import com.ximq.common.XiMQThread;
import com.ximq.common.config.ClientConfig;
import com.ximq.common.message.Request;
import com.ximq.common.message.Response;
import com.ximq.common.util.StringUtils;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @description: Consumer
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class Consumer extends Clients {

    private static ExecutorService executor = Executors.newFixedThreadPool(8);
    protected ConnectStringParser connectStringParser;
    //protected boolean partitionChanged = false;
    //protected PingCoordinator ping;

    public Consumer(Properties properties) {
        this.properties = properties;
        this.connectStringParser = new ConnectStringParser(
                properties.getProperty(ClientConfig.XIMQ_SERVER_CONNECT_STRING));
        this.serverAddresses = connectStringParser.getServerAddresses();
        inetSocketAddress = next(0);
        //ping = new PingCoordinator(this);
        //System.out.println("[Consumer] init.");
    }

    public void subscribe(Collection<String> topics, RecordFuture future) {
        for (String topic : topics) {
            ClientConfig config = firstConnect(topic, OpCode.consumerConnect, new ConsumerHandler());
            List<String> partitions = config.getPartitions();
            for (String partition : partitions) {
                try {
                    executor.submit(new SubPartition(clientNetworks, config, partition, future));
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }
    }

    static class SubPartition extends XiMQThread {

        Map<String, ClientNetwork> clientNetworks;
        ClientConfig config;
        String partition;
        String topic;
        RecordFuture future;

        public SubPartition(Map<String, ClientNetwork> clientNetworks,
                            ClientConfig config,
                            String partition,
                            RecordFuture future) {
            super("SubPartition-" + partition + "-" + config.getTopic());
            this.config = config;
            this.partition = partition;
            this.clientNetworks = clientNetworks;
            this.topic = config.getTopic();
            this.future = future;
            System.out.println("[SubPartition]-" + this.partition + "-" + this.topic);
        }

        @Override
        public void run() {
            while (true) {
                try {
                    // [SubPartition]-hello-0,0:127.0.0.1:9092:1;-hello
                    Thread.sleep(300);
                    String[] partitionInfos = this.partition.split(",");
                    String brokerInfo = partitionInfos[1];
                    String[] connectInfos = brokerInfo.split(":");
                    String ip = connectInfos[1];
                    int port  = Integer.valueOf(connectInfos[2]);
                    String connectKey = ip + ":" + port;
                    ClientNetwork clientNetwork = clientNetworks.get(connectKey);
                    if (clientNetwork == null) {
                        clientNetwork = new ClientNetwork(new InetSocketAddress(ip, port), new ConsumerHandler());
                        clientNetwork.connect();
                        clientNetworks.put(connectKey, clientNetwork);
                        System.out.printf("[connectKey]:[%s], [ip][%s], [port][%d], [partition][%s]...\n",
                                connectKey, ip, port, partitionInfos[0]);
                    }
                    Request request = new Request();
                    request.setOpCode(OpCode.consumerReceive);
                    request.setTopic(topic);
                    request.setGroupId(config.getGroupId());
                    request.setPartition(partitionInfos[0]);
                    clientNetwork.getClientHandler().subscribe(request, future);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
