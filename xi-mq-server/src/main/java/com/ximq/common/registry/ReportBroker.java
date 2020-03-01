package com.ximq.common.registry;

import com.xicp.*;
import com.ximq.common.Node;
import com.ximq.common.XiMQThread;
import com.ximq.common.config.Configuration;
import com.ximq.common.util.StringUtils;

import java.io.IOException;
import java.sql.Struct;
import java.util.List;
import java.util.stream.Stream;

/**
 * @description: ReportConfig
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class ReportBroker implements Watcher {

    Configuration configuration;
    private XiCP xc;
    ClearConfig clearConfig = new ClearConfig();

    public ReportBroker(Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.xc = new XiCP(configuration.getXicpConnnect(), this);
        this.clearConfig.start();
    }

    public void reportBroker() {
        try {
            String root = "/";
            this.createPermNode(root);
            String ximq = XiCPContent.REGISTRY_PREFIX;
            this.createPermNode(ximq);
            String broker = XiCPContent.BROKER_NODE;
            this.createPermNode(broker);
            String instance = broker + "/"
                    + configuration.getBrokerId() + ":"
                    + configuration.getListeners(); // .replaceAll("PLAINTEXT\\:\\/\\/", "")
            this.createTermNode(instance);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            if (!EventType.NodeDeleted.equals(watchedEvent.getEventType())) {
                return;
            }
            List<String> children = xc.getChildren(XiCPContent.BROKER_NODE, true);
            for (String chid : children) {
                chid = chid.substring(chid.lastIndexOf("/") + 1);
                String[] childs = chid.split(":");
                Node node = new Node(childs[1], Integer.valueOf(childs[2]), null, null);
                node.setNodeId(childs[0]);
                node.setType(Node.Type.BROKER);
                configuration.getBrokers().add(node);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void createTermNode(String path) {
        this.createNode(path, path, true);
    }

    protected void createPermNode(String path) {
        this.createNode(path, path, false);
    }

    protected void createTermNode(String path, String data) {
        this.createNode(path, data, true);
    }

    protected void createPermNode(String path, String data) {
        this.createNode(path, data, false);
    }

    protected void delNode(String path) {
        try {
            xc.delete(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void createNode(String path, String data, boolean ephemer) {
        if (path == null) {
            System.out.println("path is null...");
            return;
        }
        try {
            if (!xc.exists(path, true)) {
                xc.create(path, (data == null ? path : data).getBytes(), ephemer, false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class ClearConfig extends XiMQThread {

        public ClearConfig() {
            super("ClearConfig");
        }

        @Override
        public void run() {
            while (true) {
                try {
                    String deadClent = configuration.getDeadClient();
                    Node node = configuration.getNodeMap().get(deadClent);
                    if (Node.Type.CONSUMER.equals(node.getType())) {
                        recursionDel(node);
                        configuration.getNodeMap().remove(deadClent);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected void recursionDel(Node node) {
        // 删除 ximq consumer
        //recursionDel(, deadClent);
        List<String> cc = getChildren(XiCPContent.CONSUMER_NODE);
        for (String c : cc) {
            if (c.contains(node.getShortKey())) {
                System.out.println("需要删除节点：[" + c + "], root=[" + XiCPContent.CONSUMER_NODE + "].");
                delNode(XiCPContent.CONSUMER_NODE + "/" + c);
            }
        }
        // 删除 topic 下 consumer
        String tcp = XiCPContent.TOPIC_NODE
                + "/" + node.getTopic() + XiCPContent.CONS_GROUP_NODE
                + "/" + node.getGroupId() + XiCPContent.CONSUMER;
        List<String> tc = getChildren(tcp);
        for (String c : tc) {
            if (c.contains(node.getShortKey())) {
                System.out.println("需要删除节点：[" + c + "], root=[" + tcp + "].");
                delNode(tcp + "/" + c);
            }
        }
    }

    protected List<String> getChildren(String path) {
        if (path == null) {
            System.out.println("path is null...");
            return null;
        }
        try {
            if (xc.exists(path, true)) {
                return xc.getChildren(path, true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("getChildren Exception -> " + path);
        }
        return null;
    }

    public static void main(String[] args) {
        //String src = "PLAINTEXT://127.0.0.1:9092";
        //System.out.println(src.replaceAll("PLAINTEXT://", ""));
        //src = "/XiMQ" + "/broker" + "/0:127.0.0.1:9092";
        //System.out.println(src);
        //src = src.substring(src.lastIndexOf("/") + 1);
        //String[] childs = src.split(":");
        //Stream.of(childs).forEach(x -> System.out.println(x));


        Node na = new Node("", 0, "", "");
        na.setType(Node.Type.BROKER);
        Node nb = new Node("", 0, "", "");
        nb.setType(Node.Type.BROKER);
        System.out.println(Node.Type.BROKER.equals(nb.getType()));
    }
}
