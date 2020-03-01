package com.ximq.common;

import com.ximq.common.config.ProducerConfig;
import com.ximq.common.message.Record;
import com.ximq.common.message.Request;
import com.ximq.common.message.Response;
import com.ximq.common.network.NettyServer;
import com.ximq.common.persistent.PersistentManager;
import com.ximq.common.util.StringUtils;
import com.ximq.server.MQServer;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @description: PersistentProcesser
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class PersistentProcesser extends XiMQThread implements Processor {

    private final MQServer server;
    private final LinkedBlockingQueue<Record> queuedRequests = new LinkedBlockingQueue<>();
    private final Processor nextProcessor;

    public PersistentProcesser(MQServer server, Processor nextProcessor) {
        super("PersistentProcesser");
        this.server = server;
        this.nextProcessor = nextProcessor;
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
                        PersistentManager pm = server.getPersistentManager();
                        pm.appendLog(request);
                        response = new Response();
                        response.setOpCode(OpCode.success);
                        ns.sendResponse(response);
                        break;
                    default:
                        if (nextProcessor != null) {
                            nextProcessor.process(ns);
                        }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(Record record) throws ProcessorException {
        queuedRequests.add(record);
    }
}
