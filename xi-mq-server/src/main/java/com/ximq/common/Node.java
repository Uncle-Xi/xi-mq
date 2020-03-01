package com.ximq.common;

import com.ximq.common.util.StringUtils;

import java.net.InetSocketAddress;
import java.util.Objects;


public class Node {

    private String nodeId;
    private final String host;
    private final int port;
    private final String topic;
    private final String groupId;
    private Type type;

    public Node(InetSocketAddress inetSocketAddress, String topic, String groupId) {
        this(inetSocketAddress.getHostString(), inetSocketAddress.getPort(), topic, groupId);
    }

    public Node(String host, int port, String topic, String groupId) {
        this(null, host, port, topic, groupId, null);
    }

    public Node(String nodeId, String host, int port, String topic, String groupId, Type type) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.topic = topic;
        this.groupId = groupId;
        this.type = type;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getTopic() {
        return topic;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getShortKey() {
        return host + ":" + port;
    }

    @Override
    public String toString() {
        return host + ":" + port + ":" + topic + ":" + groupId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public enum Type{
        CONSUMER,
        PRODUCER,
        BROKER
    }
}
