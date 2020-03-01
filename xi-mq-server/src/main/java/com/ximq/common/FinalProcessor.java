package com.ximq.common;

import com.ximq.common.config.ConsumerConfig;
import com.ximq.common.message.Record;
import com.ximq.common.message.Request;
import com.ximq.common.message.Response;
import com.ximq.common.network.NettyServer;
import com.ximq.common.persistent.PersistentManager;
import com.ximq.common.util.StringUtils;
import com.ximq.server.MQServer;

/**
 * @description: FinalProcessor
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class FinalProcessor implements Processor {

    private MQServer server;
    private PersistentManager pm;

    public FinalProcessor(MQServer server) {
        this.server = server;
        this.pm = server.getPersistentManager();
    }

    @Override
    public void process(Record record) throws ProcessorException {
        NettyServer ns = (NettyServer) record;
        Request request = ns.getRequest();
        Response response = new Response();
        switch (request.getOpCode()) {
            case OpCode.consumerReceive:
                Object object = pm.readLog(request);
                if (object != null) {
                    response.setData(object);
                } else {
                    //System.out.println("查无数据...");
                }
                try {
                    ns.sendResponse(response);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            default:
        }
    }
}
