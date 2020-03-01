package com.ximq.common.registry;

/**
 * @description: XiCPContent
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class XiCPContent {

    /**
     *                                   /
     *                                  XiMQ
     *                           /     |     \     \
     *                      Topic   broker  consumer producer
     *                    [T1,T2,T3]
     *                   /        \
     *           partition         group_c/group_p
     *           [p1,p2,p3]        [g1,g2,g3]
     *             /   \           /    \
     *   topic-i,broker;replic... consumer Coordinator
     *                           /\     [消费分组管理]
     */

    public static final String CONSUMER = "/consumer";
    public static final String PRODUCER = "/producer";

    public static final String REGISTRY_PREFIX = "/XiMQ";
    public static final String BROKER_NODE = REGISTRY_PREFIX + "/broker";
    public static final String TOPIC_NODE = REGISTRY_PREFIX + "/topic";
    public static final String CONSUMER_NODE = REGISTRY_PREFIX + CONSUMER; // ip+端口+主题+消费分区
    public static final String PRODUCER_NODE = REGISTRY_PREFIX + PRODUCER; // ip+端口+主题

    public static final String CONS_GROUP_NODE = "/consumer_group"; // groupid - 消费者/Coordinator
    public static final String PROD_GROUP_NODE = "/producer_group"; // groupid - 生产者
    public static final String PARTITION_NODE = "/partition"; // partitionid - broker/replic
    public static final String REPLIC = "/broker";
    public static final String BROKER = "/replic";
    public static final String COODINATOR = "/coodinator";

}
