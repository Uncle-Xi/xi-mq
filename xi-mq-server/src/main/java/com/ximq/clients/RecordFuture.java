package com.ximq.clients;

import com.ximq.common.message.Record;
import com.ximq.common.message.Response;

/**
 * @description: RecordFuture
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public interface RecordFuture extends Record {

    void callback(Response response);
}
