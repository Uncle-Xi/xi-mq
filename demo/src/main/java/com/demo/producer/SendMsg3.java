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
 * @description: SendMeg
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class SendMsg3 {


    static Properties properties;

    static {
        properties = new Properties();
        properties.setProperty(ClientConfig.XIMQ_SERVER_CONNECT_STRING, "127.0.0.1:9092");
        properties.setProperty(ClientConfig.XIMQ_SEND_MESSAGE_ACK_MODEL, 1 + "");
        properties.setProperty(ClientConfig.XIMQ_GROUP_ID, "group-producer-2");
    }

    static volatile int i = 0;
    private static ExecutorService executor = Executors.newFixedThreadPool(3);

    public static void main(String[] args) {
        Producer producer = new Producer(properties);
        while (true) {
            try {
                executor.submit(() -> {
                    producer.send("HiMQ", "HiMQ-Test creatDirs-" + i++, new RecordFuture() {
                        @Override
                        public void callback(Response response) {
                            System.out.println("SendMsg2[response] -> " + StringUtils.getString(response));
                        }
                    });
                    if (i % 10 == 0) {
                        System.out.println("发送了 10 条消息");
                    }
                });
                Thread.sleep(3000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
