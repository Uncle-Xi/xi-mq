package com.ximq.common;

import com.ximq.common.config.AbstractConfig;
import com.ximq.common.config.ConsumerConfig;
import com.ximq.common.config.ProducerConfig;
import com.ximq.common.message.Record;
import com.ximq.common.message.Request;
import com.ximq.common.message.Response;
import com.ximq.common.network.NettyServer;
import com.ximq.common.util.StringUtils;
import com.ximq.server.MQServer;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @description: PrepProcesser
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class PrepProcessor extends XiMQThread implements Processor {

    private MQServer server;
    private final LinkedBlockingQueue<Record> queuedRequests = new LinkedBlockingQueue<>();
    private final Processor nextProcessor;
    private Map<String, ProducerConfig> producerConfigMap;
    private Map<String, ConsumerConfig> consumerConfigMap;

    public PrepProcessor(MQServer server, Processor nextProcessor) {
        super("PrepProcesser");
        this.server = server;
        this.nextProcessor = nextProcessor;
        this.producerConfigMap = server.getProducerConfigMap();
        this.consumerConfigMap = server.getConsumerConfigMap();
    }

    @Override
    public void run() {
        try {
            while (true) {
                NettyServer ns = (NettyServer) queuedRequests.take();
                Request request = ns.getRequest();
                Response response;
                switch (request.getOpCode()) {
                    case OpCode.producerSend:
                    case OpCode.consumerReceive:
                        if (nextProcessor != null) {
                            nextProcessor.process(ns);
                        }
                        break;
                    case OpCode.producerConnect:
                        response = getProdConfig(request).reportConfig(request.getConfig());
                        ns.sendResponse(getResponse(request, response));
                        break;
                    case OpCode.consumerConnect:
                        response = getConsConfig(request).reportConfig(request.getConfig());
                        ns.sendResponse(getResponse(request, response));
                        break;
                    case OpCode.producerPing:
                        response = getProdConfig(request).updateConfig(request.getConfig());
                        ns.sendResponse(getResponse(request, response));
                        break;
                    case OpCode.consumerPing:
                        response = getConsConfig(request).updateConfig(request.getConfig());
                        ns.sendResponse(getResponse(request, response));
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private ProducerConfig getProdConfig(Request request) {
        ProducerConfig config = producerConfigMap.get(request.getKey());
        if (config == null) {
            config = new ProducerConfig(server.configuration, request.getConfig());
            producerConfigMap.put(request.getKey(), config);
        }
        //if (!config.isConfigur()) {
        //    config.reportConfig(request.getConfig());
        //}
        return config;
    }


    private ConsumerConfig getConsConfig(Request request) {
        ConsumerConfig config = consumerConfigMap.get(request.getKey());
        if (config == null) {
            config = new ConsumerConfig(server, request.getConfig());
            consumerConfigMap.put(request.getKey(), config);
        }
        return config;
    }

    private Response getResponse(Request request, Response response) {
        response.setOpCode(request.getOpCode());
        return response;
    }

    @Override
    public void process(Record record) throws ProcessorException {
        queuedRequests.add(record);
    }
}
