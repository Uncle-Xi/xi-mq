package com.ximq.common;

import java.net.InetSocketAddress;
import java.util.*;


public final class Cluster {

    private final boolean isBootstrapConfigured;
    private final List<Node> nodes;
    private final Set<String> unauthorizedTopics;
    private final Set<String> invalidTopics;
    private final Set<String> internalTopics;
    private final Node controller;

    public Cluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> internalTopics) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, Collections.emptySet(), internalTopics, null);
    }


    public Cluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> internalTopics,
                   Node controller) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, Collections.emptySet(), internalTopics, controller);
    }


    public Cluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> invalidTopics,
                   Set<String> internalTopics,
                   Node controller) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller);
    }

    private Cluster(String clusterId,
                    boolean isBootstrapConfigured,
                    Collection<Node> nodes,
                    Collection<PartitionInfo> partitions,
                    Set<String> unauthorizedTopics,
                    Set<String> invalidTopics,
                    Set<String> internalTopics,
                    Node controller) {
        this.isBootstrapConfigured = isBootstrapConfigured;
        List<Node> copy = new ArrayList<>(nodes);
        Collections.shuffle(copy);
        this.nodes = Collections.unmodifiableList(copy);
        Map<Integer, Node> tmpNodesById = new HashMap<>();
        Map<Integer, List<PartitionInfo>> tmpPartitionsByNode = new HashMap<>(nodes.size());

        for (Map.Entry<Integer, List<PartitionInfo>> entry : tmpPartitionsByNode.entrySet()) {
            tmpPartitionsByNode.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
        }

        this.unauthorizedTopics = Collections.unmodifiableSet(unauthorizedTopics);
        this.invalidTopics = Collections.unmodifiableSet(invalidTopics);
        this.internalTopics = Collections.unmodifiableSet(internalTopics);
        this.controller = controller;
    }


    public static Cluster empty() {
        return new Cluster(null, new ArrayList<>(0), new ArrayList<>(0), Collections.emptySet(),
                Collections.emptySet(), null);
    }


    public static Cluster bootstrap(List<InetSocketAddress> addresses) {
        List<Node> nodes = new ArrayList<>();
        int nodeId = -1;
        return new Cluster(null, true, nodes, new ArrayList<>(0),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null);
    }

    public List<Node> nodes() {
        return this.nodes;
    }


    public Set<String> unauthorizedTopics() {
        return unauthorizedTopics;
    }

    public Set<String> invalidTopics() {
        return invalidTopics;
    }

    public Set<String> internalTopics() {
        return internalTopics;
    }

    public boolean isBootstrapConfigured() {
        return isBootstrapConfigured;
    }

    public Node controller() {
        return controller;
    }


}
