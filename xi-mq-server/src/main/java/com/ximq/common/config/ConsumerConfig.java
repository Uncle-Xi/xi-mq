package com.ximq.common.config;

import com.xicp.EventType;
import com.xicp.WatchedEvent;
import com.xicp.XiCP;
import com.ximq.common.message.Request;
import com.ximq.common.message.Response;
import com.ximq.common.registry.XiCPContent;
import com.ximq.common.util.StringUtils;
import com.ximq.server.MQServer;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.DelayQueue;

/**
 * @description: ConsumerConfig
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class ConsumerConfig extends AbstractConfig {

    private int pingSpace = 5 * 60 * 1000; // mills
    private MQServer server;

    public ConsumerConfig() { }

    public ConsumerConfig(Configuration configuration) {
        super(configuration);
        try {
            this.xc = new XiCP(configuration.getXicpConnnect(), this);
            super.initXiCPNode();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ConsumerConfig(MQServer server, ClientConfig config) {
        this(server.configuration);
        this.server = server;
        this.config = config;
        String autoOffsetReset = this.config.getAutoOffsetReset();
        if (autoOffsetReset != null && "earliest".equals(autoOffsetReset)) {
            this.config.setOffset(0);
        } else {
            this.config.setOffset(server.getPersistentManager().lastOffset());
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (!EventType.NodeChildrenChanged.equals(watchedEvent.getEventType())) {
            return;
        }
        //checkNode(this.config);
    }

    public Response reportConfig(ClientConfig config) {
        System.out.println("ConsumerConfig reportConfig -> " + StringUtils.getString(config));
        initConsumerNode(config);
        // startTimerTask(); // TODO 是 coodinator 才做这个处理
        return getConsumerConfig(config);
    }

    public Response updateConfig(ClientConfig config) {
        System.out.println("ConsumerConfig updateConfig -> " + StringUtils.getString(config));
        // 启动一个定时器，指定时间到了没有收到 ping 删除 consumer 节点
        // 三秒后开始执行，每隔一秒执行一次

        // stopTimer();
        // startTimerTask();
        return getConsumerConfig(config);
    }

    protected Timer timer;

    protected void startTimerTask() {
        //System.out.println("启动一个定时器，到时间没有被 ping 就清理掉当前消费者...");
        timer = new Timer();
        timer.schedule(new Task(timer), pingSpace);
    }

    protected void stopTimer() {
        if (timer != null) {
            timer.cancel();
        }
        //System.out.println("收到了 ping ，清理掉原来的定时器，并重新注册一个...");
    }

    class Task extends TimerTask {

        private Timer timer;

        public Task(Timer timer) {
            this.timer = timer;
        }

        @Override
        public void run() {
            //System.out.println("到时间了，没有收到 ping，清理掉 consumer 的节点...");
            delConsumer();
        }
    }

    protected void delConsumer() {
        String consumer = XiCPContent.CONSUMER_NODE + "/"
                + this.config.getInstance() + ":" + this.config.getTopic() + ":" + this.config.getGroupId();
        delNode(consumer);
    }
}
