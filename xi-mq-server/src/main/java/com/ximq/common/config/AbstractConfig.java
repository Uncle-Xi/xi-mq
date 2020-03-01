package com.ximq.common.config;

import com.xicp.Watcher;
import com.xicp.XiCP;
import com.ximq.common.message.Record;
import com.ximq.common.message.Response;
import com.ximq.common.registry.XiCPContent;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @description: AbstractConfig
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public abstract class AbstractConfig implements Record, Watcher {

    protected ClientConfig config;
    protected Response response = new Response();
    protected Configuration configuration;
    protected XiCP xc;
    protected boolean inited = false;
    protected boolean configur = false;

    protected AbstractConfig() {
    }

    protected AbstractConfig(Configuration configuration) {
        this.configuration = configuration;
    }

    public void setConfig(ClientConfig config) {
        this.config = config;
    }

    protected void initXiCPNode() {
        if (inited) {
            return;
        }
        try {
            String root = "/";
            this.createPermNode(root);
            String ximq = XiCPContent.REGISTRY_PREFIX;
            this.createPermNode(ximq);
            String topic = XiCPContent.TOPIC_NODE;
            this.createPermNode(topic);
            String consumer = XiCPContent.CONSUMER_NODE;
            this.createPermNode(consumer);
            String producer = XiCPContent.PRODUCER_NODE;
            this.createPermNode(producer);
            inited = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void checkNode(ClientConfig config) {
        if (!configur) {
            System.out.println("配置文件未加载，无效检查...");
            return;
        }
        String broker = XiCPContent.BROKER_NODE;
        List<String> brokers = getChildren(broker); // 0:127.0.0.1:9092:2
        List<String> partitionList = getPartitionList(config);
        boolean dataChange = false;
        // partition 下的 broker 都出现在 broker 节点中
        for (String p : partitionList) {
            boolean have = false;
            p = p.split(",")[1];
            String[] ar = p.split(":");
            p = ar[1] + ":" + ar[2];
            for (String b : brokers) {
                if (b.contains(p)) {
                    have = true;
                }
            }
            if (!have) {
                System.out.println("数据变化了 p -> " + p);
                dataChange = true;
                break;
            }
        }
        String consumer = XiCPContent.CONSUMER_NODE;
        List<String> consumers = getChildren(consumer); // 127.0.0.1:6789:topic:group-01
        String gcgconsumer = XiCPContent.TOPIC_NODE + "/" + config.getTopic()
                + XiCPContent.CONS_GROUP_NODE + "/" + config.getGroupId() + XiCPContent.CONSUMER;
        List<String> gcgconsumerList = getChildren(gcgconsumer); // 127.0.0.1:6789:topic@topic-0,0:127.0.0.1:9092:2&
        // gcg consumer 节点下的 consumer 都出现在了 consumer 节点中
        for (String gc : gcgconsumerList) {
            boolean have = false;
            gc = gc.split("@")[0];
            for (String c : consumers) {
                if (c.contains(gc)) {
                    have = true;
                    break;
                }
            }
            if (!have) {
                System.out.println("数据变化了 gc -> " + gc);
                dataChange = true;
                break;
            }
        }
        if (dataChange) {
            delOldNode(config);
            configur = false;
            initConsumerNode(config);
        }
    }

    protected List<String> getPartitionList(ClientConfig config) {
        String partitionPath = XiCPContent.TOPIC_NODE + "/" +
                config.getTopic() + XiCPContent.PARTITION_NODE;
        return getChildren(partitionPath); // topic-0,0:127.0.0.1:9092:2
    }

    protected void delOldNode(ClientConfig config) {
        String rootNode = XiCPContent.TOPIC_NODE + "/" + config.getTopic();
        System.out.println("[递归删除历史节点], node ROOT -> " + rootNode);
        recursionDel(rootNode);
    }

    protected void recursionDel(String path) {
        List<String> children = getChildren(path);
        if (children == null || children.size() < 1) {
            delNode(path);
        } else {
            for (String c : children) {
                recursionDel(path + "/" + c);
            }
        }
    }

    public static void main(String[] args) {
        //String consumer = "192.168.0.12:8899:topic@topic-0,0:192.168.7.1:9092:10;&topic-1,0:192.168.7.1:9092:10;";
        //String instance = "192.168.0.12:8899";
        //System.out.println(consumer.contains(instance));
        //String[] parArr = consumer.split("@")[1].split("&");
        //Stream.of(parArr).forEach(p -> System.out.println(p));
        String[] consumers = {"99", "888", "777"};
        String[] gcgconsumerList = {"99",};
        for (String gc : gcgconsumerList) {
            boolean have = false;
            gc = gc.split("@")[0];
            for (String c : consumers) {
                if (c.contains(gc)) {
                    have = true;
                    break;
                }
            }
            if (!have) {
                System.out.println("数据变化了 gc -> " + gc);
                break;
            }
            System.out.println("ok");
        }
    }

    protected synchronized void initConsumerNode(ClientConfig config) {
        if (configur) {
            System.out.println("配置文件已经加载完毕...");
            return;
        }
        String topic = config.getTopic();
        String consumerInstance = XiCPContent.CONSUMER_NODE + "/" + config.getInstance() + ":" + topic + ":" + config.getGroupId();
        this.createTermNode(consumerInstance);

        String topicNode = XiCPContent.TOPIC_NODE + "/" + topic;
        this.createPermNode(topicNode);

        String consumer_group = topicNode + XiCPContent.CONS_GROUP_NODE;
        this.createPermNode(consumer_group);

        String consumer_group_gid = consumer_group + "/" + config.getGroupId();
        this.createPermNode(consumer_group_gid);

        String consumer_group_pNode = consumer_group_gid + XiCPContent.CONSUMER;
        this.createPermNode(consumer_group_pNode);

        String consumer_group_cInstance = consumer_group_pNode + "/" + config.getInstance() + ":" + topic + "@";
        this.createTermNode(consumer_group_cInstance);

        //System.out.println("[initConsumerNode] [topic] -> " + topic);
        //System.out.println("[initConsumerNode] [topicNode] -> " + topicNode);
        //System.out.println("[initConsumerNode] [consumerInstance] -> " + consumerInstance);
        //System.out.println("[initConsumerNode] [consumer_group] -> " + consumer_group);
        //System.out.println("[initConsumerNode] [consumer_group_gid] -> " + consumer_group_gid);
        //System.out.println("[initConsumerNode] [consumer_group_cNode] -> " + consumer_group_pNode);
        //System.out.println("[initConsumerNode] [consumer_group_cInstance] -> " + consumer_group_cInstance);

        this.clearDeadClient(config);
        this.partitionConfig(topic);
        this.coodinatorConfig(consumer_group_gid);
        this.topicConsumerNodeUpdate(consumer_group_pNode, topic);
        configur = true;
    }

    protected void clearDeadClient(ClientConfig config){
//        Set<String> handled = new HashSet<>();
//        Set<String> clients = configuration.getDeadClient();
//        System.out.println("[clearDeadClient] [待处理死亡客户端数量] -> " + clients.size());
//        if (clients == null || clients.size() < 1) {
//            System.out.println("[clearDeadClient] [无死亡消费者] -> " + clients.size());
//            return;
//        }
//        String cgcNode = XiCPContent.TOPIC_NODE + "/" + config.getTopic()
//                + XiCPContent.CONS_GROUP_NODE + "/" + config.getGroupId() + XiCPContent.CONSUMER;
//        List<String> cgNodeChilds = getChildren(cgcNode);
//        for (String consumer : cgNodeChilds) {
//            for (String client : clients) {
//                if (consumer.contains(client)) {
//                    delNode(cgcNode + "/" + consumer);
//                    handled.add(client);
//                }
//            }
//        }
//        System.out.println("[clearDeadClient] [清理topic下消费者组，死亡消费者数量] -> " + handled.size());
//        String consumerNode = XiCPContent.CONSUMER_NODE;
//        List<String> consumerNodeChilds = getChildren(consumerNode);
//        for (String consumer : consumerNodeChilds) {
//            for (String client : clients) {
//                if (consumer.contains(client)) {
//                    delNode(consumerNode + "/" + consumer);
//                    handled.add(client);
//                }
//            }
//        }
//        System.out.println("[clearDeadClient] [清理root消费者组，死亡消费者数量] -> " + handled.size());
//        configuration.getDeadClient().removeAll(handled);
    }

    protected void topicConsumerNodeUpdate(String consumer_group_pNode, String topic) {
        String partitionNode = XiCPContent.TOPIC_NODE + "/" + topic + XiCPContent.PARTITION_NODE;
        List<String> consumers = getChildren(consumer_group_pNode);
        List<String> partitions = getChildren(partitionNode);
        System.out.println("[partitionNode][" + partitionNode + "], [consumers.size][" + consumers.size()
                + "], [partitions.xize][" + partitions.size() + "].");
        for (int i = 0; i < partitions.size(); i++) {
            String consumer = consumers.get(i % consumers.size());
            delNode(consumer_group_pNode + "/" + consumer);
            String[] carr = consumer.split("@");
            String ci = carr[0];
            String cp = carr.length > 1 ? carr[1] + "&" : "";
            String cpn = consumer_group_pNode + "/" + ci + "@" + cp + partitions.get(i);
            createTermNode(cpn);
            System.out.println("[topicConsumerNodeUpdate] cpn -> " + cpn);
        }
    }

    protected void coodinatorConfig(String consumer_group_gid) {
        String broker = XiCPContent.BROKER_NODE;
        List<String> brokers = getChildren(broker);
        List<Broker> bl = sortBrokerList(brokers);
        Broker b = bl.get(0);
        createPermNode(consumer_group_gid);

        String coordinatorNode = consumer_group_gid + XiCPContent.COODINATOR;
        createPermNode(coordinatorNode);

        String coordinatorInstance = coordinatorNode + "/" + b.pprefix;
        createTermNode(coordinatorInstance);

        System.out.println("[coodinatorConfig] [getChildren(coordinator)] -> " + getChildren(coordinatorNode));
    }

    protected void partitionConfig(String topic) {
        int numPartitions = configuration.getNumPartitions();
        String broker = XiCPContent.BROKER_NODE;
        String partitionNode = XiCPContent.TOPIC_NODE + "/" + topic + XiCPContent.PARTITION_NODE;
        List<String> brokers = getChildren(broker);
        List<String> partitionInfos = getChildren(partitionNode);
        if (!exsits(partitionNode) || partitionInfos == null || partitionInfos.size() < 1) {
            System.out.println("[partitionConfig] [当前未设置分区] [开始设置分区] - " + partitionNode);
            this.createPermNode(partitionNode);
            List<Broker> bl = sortBrokerList(brokers);
            int bs = bl.size();
            for (int i = 0; i < numPartitions; i++) {
                Broker bro = bl.get(i % bs);
                bro.load = bro.load + 1;
                String obn = broker + "/" + bro.path;
                delNode(obn);
                bro.path = bro.pprefix + bro.load;
                String nbn = broker + "/" + bro.path;
                createTermNode(nbn);
                // 这一部分与主题提交分开，单方面一次提交 TODO 避免重复

                String pps = partitionNode + "/" + topic + "-" + i + "," + bro.path + ";";
                createTermNode(pps);
            }
            System.out.println("[partitionConfig] [end]...");
            // TODO 创建副本集合
            // 不含当前分区的broker，负载最小的broker，循环分配
        }
        System.out.println("[partitionConfig] [topic 分区] - " + getChildren(partitionNode));
    }

    protected List<Broker> sortBrokerList(List<String> brokers) {
        // 创建分区
        List<Broker> bl = new ArrayList<>();
        for (String bro : brokers) {
            // 找到负载最小的 broker 列表，根据负载排序
            // 创建列表，插入排序，由小到大
            String[] broArr = bro.split(":");
            int load = 0;
            if (broArr.length > 3) {
                load = Integer.valueOf(broArr[3].trim());/*bro.substring(0, bro.lastIndexOf(":"))*/
            }
            String pprefix = broArr[0] + ":" + broArr[1] + ":" + broArr[2] + ":";
            bl.add(new Broker(load, pprefix, bro));
        }
        int idx = 0;
        Broker b = bl.get(idx);
        for (int i = 0; i < bl.size() - 1; i++) {
            for (int k = i + 1; k >= 0; k--) {
                if (bl.get(i).load > bl.get(k).load) {
                    idx = i;
                    b = bl.get(k);
                    bl.set(k, bl.get(i));
                }
            }
        }
        bl.set(idx, b);
        return bl;
    }

    static class Broker {
        int load;
        String pprefix;
        String path;

        public Broker(int load, String pprefix, String path) {
            this.load = load;
            this.pprefix = pprefix;
            this.path = path;
        }
    }

    public List<String> rebuildingNnode(ClientConfig config, String cgnp){
        System.err.println("[getConsumerConfig][rebuildingNnode] -> " + cgnp);
        initConsumerNode(config);
        return getChildren(cgnp);
    }

    protected Response getConsumerConfig(ClientConfig config) {
        this.clearDeadClient(config);
        String consumer_group_cNode = XiCPContent.TOPIC_NODE + "/" + config.getTopic()
                + XiCPContent.CONS_GROUP_NODE + "/" + config.getGroupId() + XiCPContent.CONSUMER;
        List<String> cpb = getChildren(consumer_group_cNode);
        System.out.printf("[getConsumerConfig][consumer_group_cNode]:[%s], [consumer_partition_broker][%d].\n",
                consumer_group_cNode, cpb == null ? 0 : cpb.size());
        if (cpb == null || cpb.size() < 1) {
            cpb = rebuildingNnode(config, consumer_group_cNode);
        }
        for (String consumer : cpb) {
            //System.out.println("[getConsumerConfig] [for - consumer] -> " + consumer);
            if (consumer.contains(config.getInstance())) {
                if (consumer.split("@").length < 2) {
                    //System.out.println("[" + consumer + "] - 数据不符合要求...【怎么来的？】");
                    delNode(consumer);
                    continue;
                }
                String[] parInfos = consumer.split("@")[1].split("&");
                Stream.of(parInfos).forEach(pi -> {
                    if (pi != null && !"".equals(pi) && !"null".equals(pi)) {
                        this.config.getPartitions().add(pi);
                    }
                });
            }
        }
        String coordinator = XiCPContent.TOPIC_NODE + "/" + config.getTopic()
                + XiCPContent.CONS_GROUP_NODE + "/" + config.getGroupId() + XiCPContent.COODINATOR;
        //System.out.println("[getConsumerConfig][coordinator] -> " + coordinator);
        List<String> coord = getChildren(coordinator);
        //System.out.println("[getConsumerConfig][getChildren(coordinator)] -> " + coord);
        this.config.setCoordinator(coord == null ? null : coord.size() > 0 ? coord.get(0) : null);
        response.setConfig(this.config);
        return response;
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
            System.out.println("createNode Exception -> " + path);
        }
    }

    protected boolean exsits(String path) {
        if (path == null) {
            System.out.println("path is null...");
            return false;
        }
        try {
            return xc.exists(path, true);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("exsits Exception -> " + path);
        }
        return false;
    }

    protected void delNode(String path) {
        if (path == null) {
            System.out.println("path is null...");
            return;
        }
        try {
            if (xc.exists(path, true)) {
                xc.delete(path);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("delNode Exception -> " + path);
        }
    }

    protected String getData(String path) {
        if (path == null) {
            System.out.println("path is null...");
            return null;
        }
        try {
            if (xc.exists(path, true)) {
                return xc.getData(path, true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("getData Exception -> " + path);
        }
        return null;
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
            // TODO /XiMQ/topic/hello/consumer_group/group-consumer-1/consumer
        }
        return null;
    }

    public boolean isConfigur() {
        return configur;
    }

    public void setConfigur(boolean configur) {
        this.configur = configur;
    }
}
