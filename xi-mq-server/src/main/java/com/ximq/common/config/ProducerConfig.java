package com.ximq.common.config;

import com.xicp.WatchedEvent;
import com.xicp.XiCP;
import com.ximq.common.message.Response;
import com.ximq.common.registry.XiCPContent;
import com.ximq.common.util.StringUtils;

import java.util.List;

/**
 * @description: ProducerConfig
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class ProducerConfig extends AbstractConfig {

    public ProducerConfig() {
    }

    public ProducerConfig(Configuration configuration, ClientConfig config) {
        super(configuration);
        try {
            this.xc = new XiCP(configuration.getXicpConnnect(), this);
            super.initXiCPNode();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("无动作...");
    }

    public Response reportConfig(ClientConfig config) {
        //System.out.println("reportConfig, 参数：" + StringUtils.getString(config));
        this.config = config;
        // 注册 producer
        String producerInstace = "/" + config.getInstance() + ":" + config.getTopic() + ":" + config.getGroupId();
        //System.out.println("ProducerConfig:reportConfig -> " + producerInstace);
        String producer = XiCPContent.PRODUCER_NODE + producerInstace;
        createTermNode(producer);
        // 注册 group_producer
        String topicp = XiCPContent.TOPIC_NODE + "/" + config.getTopic();
        this.createPermNode(topicp);
        String groupp = topicp + XiCPContent.PROD_GROUP_NODE;
        this.createPermNode(groupp);
        String grouppg = groupp + "/" + config.getGroupId();
        this.createPermNode(grouppg);
        String grouptc = grouppg + producerInstace;
        this.createPermNode(grouptc);
        // 触发分区创建
        this.partitionConfig(config.getTopic());
        return this.updateConfig(config);
    }

    public Response updateConfig(ClientConfig config) {
        List<String> partitionList = getPartitionList(config);
        config.setPartitions(partitionList);
        response.setConfig(config);
        return response;
    }

}
