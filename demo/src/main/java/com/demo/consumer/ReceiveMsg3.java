package com.demo.consumer;

import com.ximq.clients.RecordFuture;
import com.ximq.clients.consumer.Consumer;
import com.ximq.common.config.ClientConfig;
import com.ximq.common.message.Response;
import com.ximq.common.util.StringUtils;

import java.util.Collections;
import java.util.Properties;

/**
 * @description: receive msg
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class ReceiveMsg3 {

    static Properties properties;

    static {
        properties = new Properties();
        properties.setProperty(ClientConfig.XIMQ_SERVER_CONNECT_STRING, "127.0.0.1:9092");
        properties.setProperty(ClientConfig.XIMQ_SEND_MESSAGE_ACK_MODEL, 1 + "");
        properties.setProperty(ClientConfig.XIMQ_GROUP_ID, "group-consumer-HiMQ");
    }

    public static void main(String[] args) {
        Consumer consumer = new Consumer(properties);
        consumer.subscribe(Collections.singleton("HiMQ"), new RecordFuture() {
            @Override
            public void callback(Response response) {
                System.out.println("HiMQ[response] -> " + StringUtils.getString(response));
            }
        });
    }
}
