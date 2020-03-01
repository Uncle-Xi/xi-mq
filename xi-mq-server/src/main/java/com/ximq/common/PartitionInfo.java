package com.ximq.common;


public class PartitionInfo {
    private final String topic;
    private final int partition;
    private final Node leader;
    private final Node[] replicas;
    private final Node[] inSyncReplicas;
    private final Node[] offlineReplicas;

    public PartitionInfo(String topic, int partition, Node leader, Node[] replicas, Node[] inSyncReplicas) {
        this(topic, partition, leader, replicas, inSyncReplicas, new Node[0]);
    }

    public PartitionInfo(String topic,
                         int partition,
                         Node leader,
                         Node[] replicas,
                         Node[] inSyncReplicas,
                         Node[] offlineReplicas) {
        this.topic = topic;
        this.partition = partition;
        this.leader = leader;
        this.replicas = replicas;
        this.inSyncReplicas = inSyncReplicas;
        this.offlineReplicas = offlineReplicas;
    }


    public String topic() {
        return topic;
    }


    public int partition() {
        return partition;
    }


    public Node leader() {
        return leader;
    }


    public Node[] replicas() {
        return replicas;
    }


    public Node[] inSyncReplicas() {
        return inSyncReplicas;
    }


    public Node[] offlineReplicas() {
        return offlineReplicas;
    }


}
