package com.ximq.common;

import com.ximq.common.config.ClientConfig;
import com.ximq.common.message.Record;
import com.ximq.common.message.Request;
import com.ximq.common.message.Response;
import com.ximq.common.network.NettyServer;
import com.ximq.common.util.StringUtils;
import com.ximq.server.MQServer;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @description: SyncProcesser
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class SyncProcesser extends XiMQThread implements Processor {

    private final MQServer server;
    private final LinkedBlockingQueue<Record> queuedRequests = new LinkedBlockingQueue<>();
    private final Processor nextProcessor;

    public SyncProcesser(MQServer server, Processor nextProcessor) {
        super("SyncProcesser");
        this.server = server;
        this.nextProcessor = nextProcessor;
    }

    @Override
    public void run() {
        try {
            while (true){
                NettyServer ns = (NettyServer) queuedRequests.take();
//                Request request = ns.getRequest();
//                Response response;
//                switch (request.getOpCode()) {
//                    case OpCode.producerSend:
//                        if (request.getConfig() != null){
//                            ClientConfig config = request.getConfig();
//                            if (config.getAck() == 0) {
//                                // 生产者不关心结果
//                            } else if (config.getAck() == 1) {
//                                // 当前主分区保存即可
//                            } else {
//                                // TODO 所有 ISR 副本集需要完成同步
//                            }
//                        } else {
//                            // 1 当前主分区保存即可
//                        }
//                        break;
//                    case OpCode.consumerReceive:
//                        break;
//                }
                if (nextProcessor != null) {
                    nextProcessor.process(ns);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void process(Record record) throws ProcessorException {
        queuedRequests.add(record);
    }
}
