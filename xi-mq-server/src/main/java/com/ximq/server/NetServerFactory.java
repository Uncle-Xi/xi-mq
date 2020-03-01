package com.ximq.server;

import com.ximq.common.config.Configuration;
import com.ximq.common.network.NettyServerFactory;

/**
 * @description: NetServerFactory
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public abstract class NetServerFactory {

    protected Configuration configuration;

    public void configure(Configuration configuration){
        this.configuration = configuration;
    }

    public static NetServerFactory createServerNetFactory(){
        return new NettyServerFactory();
    }

    public abstract void setServer(MQServer server);

    public abstract void start();
}
