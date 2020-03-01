package com.ximq.server;

import com.ximq.common.message.Record;
import com.ximq.common.message.Request;

import java.io.IOException;

public abstract class NetServer implements Record {

    protected Request request;

    public Request getRequest() {
        return request;
    }

    public void setRequest(Request request) {
        this.request = request;
    }

    public abstract void receiveMessage();

    public abstract void sendResponse(Record record) throws IOException;
}
