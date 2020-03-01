package com.ximq.common.config;

import com.ximq.common.Node;
import com.ximq.common.XiMQThread;
import com.ximq.common.message.Record;
import com.ximq.common.util.TransferFile;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @description: configuration
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class Configuration implements Record {

    private long logSegmentBytes;
    private int logRetentionHours;
    private String xicpConnnect;
    private String logDirs;
    private int numPartitions;
    private int brokerId;
    private String host;
    private int port;
    private String listeners;
    private Set<Node> brokers = new HashSet<>();
    private LinkedBlockingQueue<String> deadClients = new LinkedBlockingQueue<>();
    private Map<String, Node> nodeMap = new HashMap<>();

    public void parse(String path) throws Exception {
        File configFile = TransferFile.getTransferFile(new File(path), path);
        System.out.println("Reading configuration from: " + configFile);
        try {
            if (!configFile.exists()) {
                throw new IllegalArgumentException(configFile.toString() + " file is missing");
            }
            Properties cfg = new Properties();
            FileInputStream in = new FileInputStream(configFile);
            try {
                cfg.load(in);
            } finally {
                in.close();
            }
            parseProperties(cfg);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            TransferFile.deleteFile(configFile);
        }
    }

    public void parseProperties(Properties cpProp) throws Exception {
        for (Map.Entry<Object, Object> entry : cpProp.entrySet()) {
            String key = entry.getKey().toString().trim();
            String value = entry.getValue().toString().trim();
            if (key.equals("log.dirs")) {
                logDirs = value;
            } else if (key.equals("xicp.connect")) {
                xicpConnnect = value;
            } else if (key.equals("log.segment.bytes")) {
                logSegmentBytes = Long.parseLong(value);
            } else if (key.equals("log.retention.hours")) {
                logRetentionHours = Integer.parseInt(value);
            } else if (key.equals("listeners")) {
                listeners = substringLitener(value);
            } else if (key.equals("num.partitions")) {
                numPartitions = Integer.parseInt(value);
            } else if (key.equals("server.id")) {
                brokerId = Integer.parseInt(value);
            }
        }
    }

    // listeners=PLAINTEXT://127.0.0.1:9092
    private String substringLitener(String listener) {
        listener = listener.trim();
        listener = listener.substring(listener.lastIndexOf("//") + 2);
        String[] pi = listener.split(":");
        this.host = pi[0];
        this.port = Integer.valueOf(pi[1]);
        return listener;
    }

    public long getLogSegmentBytes() {
        return logSegmentBytes;
    }

    public int getLogRetentionHours() {
        return logRetentionHours;
    }

    public String getXicpConnnect() {
        return xicpConnnect;
    }

    public String getLogDirs() {
        return logDirs;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public Set<Node> getBrokers() {
        return brokers;
    }

    public void setBrokers(Set<Node> brokers) {
        this.brokers = brokers;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getListeners() {
        return listeners;
    }

    public String getDeadClient() throws InterruptedException {
        System.out.println("getDeadClient...");
        return deadClients.take();
    }

    public void addDeadClient(String deadClient) {
        System.out.println("deadClient 添加成功...");
        deadClients.add(deadClient);
    }

    public Map<String, Node> getNodeMap() {
        return nodeMap;
    }
}
