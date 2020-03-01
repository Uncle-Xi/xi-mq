package com.ximq.clients;

import com.ximq.clients.consumer.ConsumerHandler;
import com.ximq.clients.producer.ProducerHandler;
import com.ximq.common.OpCode;
import com.ximq.common.config.ClientConfig;
import com.ximq.common.message.Request;
import com.ximq.common.message.Response;
import com.ximq.common.util.StringUtils;

import java.net.InetSocketAddress;
import java.util.*;

/**
 * @description: Clients
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public abstract class Clients {

    protected Map<String, ClientNetwork> clientNetworks = new HashMap<>();
    protected InetSocketAddress inetSocketAddress;
    protected Properties properties;
    protected Map<String, ClientConfig> configMap = new HashMap<>();
    protected ArrayList<InetSocketAddress> serverAddresses;

    private int lastIndex = -1;
    private int currentIndex = -1;

    protected InetSocketAddress next(long spinDelay) {
        currentIndex = ++currentIndex % serverAddresses.size();
        if (currentIndex == lastIndex && spinDelay > 0) {
            try {
                Thread.sleep(spinDelay);
            } catch (InterruptedException e) {
                System.out.println("Unexpected exception" + e);
            }
        } else if (lastIndex == -1) {
            lastIndex = 0;
        }
        return serverAddresses.get(currentIndex);
    }

    protected ClientConfig firstConnect(String topic, int opCode, ClientHandler handler) {
        System.out.println("[firstConnect][start]...");
        Request request = new Request();
        //ProducerHandler handler = new ProducerHandler();
        ClientNetwork network = new ClientNetwork(inetSocketAddress, handler);
        ClientConfig config = getConfig(properties);
        config.setTopic(topic);

        String initNetKey = inetSocketAddress.getHostName() + ":" + inetSocketAddress.getPort();
        network.connect();
        configMap.put(topic, config);
        clientNetworks.put(initNetKey, network);

        request.setConfig(config);
        request.setOpCode(opCode);
        Response response = handler.getPartition(request);

        ClientConfig respConfig = response.getConfig();
        List<String> partitions = respConfig.getPartitions(); // topic-0,0:127.0.0.1:9092:2
        System.out.println("[firstConnect] - [partitions.size] -> " + partitions.size());
        config.setPartitions(partitions);
        config.setCoordinator(respConfig.getCoordinator());
        System.out.println(topic + " ][首次连接请求：" + StringUtils.getString(request));
        System.out.println(topic + " ][首次连接响应：" + StringUtils.getString(response));
        return config;
    }

    protected ClientConfig getConfig(Properties properties) {
        ClientConfig config = new ClientConfig();
        //config.setTopic(properties.getProperty(ClientConfig.XIMQ_TOPIC_NAME));
        config.setGroupId(properties.getProperty(ClientConfig.XIMQ_GROUP_ID));
        String ack = properties.getProperty(ClientConfig.XIMQ_SEND_MESSAGE_ACK_MODEL);
        config.setAck(ack == null ? 1 : Integer.valueOf(ack));
        config.setAutoOffsetReset(properties.getProperty(ClientConfig.AUTO_OFFSET_RESET_CONFIG));
        config.setAutoCommit(properties.getProperty(ClientConfig.ENABLE_AUTO_COMMIT_CONFIG));
        config.setConnectString(properties.getProperty(ClientConfig.XIMQ_SERVER_CONNECT_STRING));
        return config;
    }
}
