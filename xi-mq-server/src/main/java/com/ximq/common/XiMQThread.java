package com.ximq.common;

/**
 * @description: XiKVThread
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class XiMQThread extends Thread {

    public static XiMQThread daemon(final String name, Runnable runnable) {
        return new XiMQThread(name, runnable, true);
    }

    public static XiMQThread nonDaemon(final String name, Runnable runnable) {
        return new XiMQThread(name, runnable, false);
    }

    public XiMQThread(final String name) {
        super(name);
        configureThread(name, false);
    }

    public XiMQThread(final String name, boolean daemon) {
        super(name);
        configureThread(name, daemon);
    }

    public XiMQThread(final String name, Runnable runnable, boolean daemon) {
        super(runnable, name);
        configureThread(name, daemon);
    }

    private void configureThread(final String name, boolean daemon) {
        setDaemon(daemon);
        setUncaughtExceptionHandler((t, e) -> System.out.println("Uncaught exception in thread " + name + ":" + e));
    }

}