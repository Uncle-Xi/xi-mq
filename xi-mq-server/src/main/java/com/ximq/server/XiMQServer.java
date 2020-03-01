package com.ximq.server;

import com.ximq.common.cache.LRUCache;
import com.ximq.common.config.Configuration;
import com.ximq.common.persistent.PersistentManager;
import com.ximq.common.registry.ReportBroker;

import java.io.IOException;

/**
 * @description: XiMQServer
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class XiMQServer {

    /**
     * 1、解析配置文件
     * 2、启动网络服务，暴露 9092
     * 3、连接 xicp 创建节点
     * 4、初始化任务处理责任链
     */
    public static void main(String[] args) {
        XiMQServer main = new XiMQServer();
        try {
            main.initializeAndRun(args);
        } catch (Exception e) {
            System.err.println(e);
            System.exit(2);
        }
        System.exit(0);
    }

    protected void initializeAndRun(String[] args) throws Exception {
        Configuration config = new Configuration();
        if (args.length == 1) {
            config.parse(args[0]);
        }
        runFromConfig(config);
    }

    private MQServer server;

    public void runFromConfig(Configuration config) throws IOException {
        System.out.println("Starting runFromConfig...");
        try {
            server = new MQServer();
            NetServerFactory nsf = NetServerFactory.createServerNetFactory();
            nsf.configure(config);
            nsf.setServer(server);
            server.setConfiguration(config);
            server.setNetServerFactory(nsf);
            server.setReport(new ReportBroker(config));
            server.setCache(new LRUCache(10000));
            server.setPersistentManager(new PersistentManager(config, server));
            server.start();
            server.join();
        } catch (Exception e) {
            System.err.println("XiMQServer interrupted " + e);
            e.printStackTrace();
        }
    }
}
