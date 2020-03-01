package com.ximq.common.config;

import com.ximq.common.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * @description: ClientConfig
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class ClientConfig {

    public static final String AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset";
    public static final String ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit";
    public static final String XIMQ_SERVER_CONNECT_STRING = "xi.mq.servers.connect.string";
    public static final String XIMQ_TOPIC_NAME = "xi.mq.topic.name";
    public static final String XIMQ_GROUP_ID = "xi.mq.group.id";
    public static final String XIMQ_SEND_MESSAGE_ACK_MODEL = "xi.mq.send.message.ack.model";

    private Node node;
    private String connectString;
    private String topic;           // 消息主题
    private String groupId;         // groupId <> partition 多对多
    private String instance;        // ip+port
    private List<String> partitions = new ArrayList<>();
    private String coordinator;
    private String autoOffsetReset; // earliest,latest
    private String autoCommit;
    private long offset;
    private int ack = 1;

    public int getAck() {
        return ack;
    }

    public void setAck(int ack) {
        this.ack = ack;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public void setAutoOffsetReset(String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
    }

    public String getAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(String autoCommit) {
        this.autoCommit = autoCommit;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<String> partitions) {
        this.partitions = partitions;
    }

    public String getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(String coordinator) {
        this.coordinator = coordinator;
    }

    public String getConnectString() {
        return connectString;
    }

    public void setConnectString(String connectString) {
        this.connectString = connectString;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }
}
