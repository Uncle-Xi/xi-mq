package com.demo.producer;

import com.ximq.clients.RecordFuture;
import com.ximq.clients.producer.Producer;
import com.ximq.common.config.ClientConfig;
import com.ximq.common.message.Response;
import com.ximq.common.util.StringUtils;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @description: send msg
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class Send {
    static Properties properties;
    static {
        properties = new Properties();
        properties.setProperty(ClientConfig.XIMQ_SERVER_CONNECT_STRING, "127.0.0.1:9092");
        properties.setProperty(ClientConfig.XIMQ_SEND_MESSAGE_ACK_MODEL, 1 + "");
        properties.setProperty(ClientConfig.XIMQ_GROUP_ID, "group-producer-send");
    }
    public static void main(String[] args) {
        Producer producer = new Producer(properties);
        producer.send("topic", "send-data", new RecordFuture() {
            @Override
            public void callback(Response response) {
                System.out.println("【Send Message Response】-> " + StringUtils.getString(response));
            }
        });
    }
}
