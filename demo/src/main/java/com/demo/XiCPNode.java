package com.demo;

import com.xicp.WatchedEvent;
import com.xicp.Watcher;
import com.xicp.XiCP;

import java.util.List;

/**
 * @description: xicp node
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class XiCPNode implements Watcher {

    public static void main(String[] args) throws Exception {
        String partition = "/XiMQ/topic/hello/partition";
        String pson = "/XiMQ/topic/hello/partition/hello-0,0:127.0.0.1:9092:1;";
        XiCP xc = new XiCP("127.0.0.1:2181", new XiCPNode());
        System.out.println(xc.exists(partition, true));
        xc.create(pson, pson.getBytes(), true, false);
        List<String> strList = xc.getChildren(partition, true);
        strList.stream().forEach(x -> { System.out.println("hello -> " + x); });
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        // TODO
    }
}
