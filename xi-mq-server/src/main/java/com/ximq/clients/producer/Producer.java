package com.ximq.clients.producer;

import com.ximq.clients.ClientNetwork;
import com.ximq.clients.Clients;
import com.ximq.clients.ConnectStringParser;
import com.ximq.clients.RecordFuture;
import com.ximq.common.OpCode;
import com.ximq.common.config.ClientConfig;
import com.ximq.common.message.Request;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * @description: Producer
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class Producer extends Clients {

    //private ProducerHandler handler;
    //private ClientNetwork network;
    //private ClientConfig config;
    private ConnectStringParser connectStringParser;

    public Producer(Properties properties) {
        initConfigAndConnectXiMQServer(properties);
    }

    public void initConfigAndConnectXiMQServer(Properties properties) {
        this.connectStringParser = new ConnectStringParser(
                properties.getProperty(ClientConfig.XIMQ_SERVER_CONNECT_STRING));
        this.serverAddresses = connectStringParser.getServerAddresses();
        this.properties = properties;
        this.connectStringParser = new ConnectStringParser(
                properties.getProperty(ClientConfig.XIMQ_SERVER_CONNECT_STRING));
        this.serverAddresses = connectStringParser.getServerAddresses();
        inetSocketAddress = next(0);
        System.out.println("[Producer] init.");
    }

    public void send(String topic, Object data, RecordFuture future) {
        ClientConfig config = configMap.get(topic);
        if (config == null) {
            config = firstConnect(topic, OpCode.producerConnect, new ProducerHandler());
            configMap.put(topic, config);
            System.out.println("[Producer] 初始化 topic 配置完成...");
        }
        String patition = loadBalance(config.getPartitions());
        String[] partitionInfos = patition.split(",");
        String brokerInfo = partitionInfos[1];
        String[] connectInfos = brokerInfo.split(":");
        String ip = connectInfos[1];
        int port = Integer.valueOf(connectInfos[2]);
        String connectKey = ip + ":" + port;
        ClientNetwork clientNetwork = clientNetworks.get(connectKey);
        if (clientNetwork == null) {
            clientNetwork = new ClientNetwork(new InetSocketAddress(ip, port), new ProducerHandler());
            clientNetwork.connect();
            clientNetworks.put(connectKey, clientNetwork);
            System.out.printf("[connectKey]:[%s], [ip][%s], [port][%d], [partition][%s]...\n",
                    connectKey, ip, port, partitionInfos[0]);
        }
        Request request = new Request();
        request.setOpCode(OpCode.producerSend);
        request.setData(data);
        request.setTopic(topic);
        request.setGroupId(config.getGroupId());
        request.setPartition(partitionInfos[0]); // topic-0,0:127.0.0.1:9092:0;
        clientNetwork.getClientHandler().send(request, future);
    }

    public String loadBalance(List<String> partitions) {
        if (partitions.size() == 0) {
            throw new RuntimeException("无有效连接...");
        }
        int random = new Random().nextInt(partitions.size());
        return partitions.get(random);
    }
}
