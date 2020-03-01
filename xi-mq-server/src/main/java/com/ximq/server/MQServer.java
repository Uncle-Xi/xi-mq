package com.ximq.server;

import com.ximq.common.*;
import com.ximq.common.cache.Cache;
import com.ximq.common.config.Configuration;
import com.ximq.common.config.ConsumerConfig;
import com.ximq.common.config.ProducerConfig;
import com.ximq.common.persistent.PersistentManager;
import com.ximq.common.registry.ReportBroker;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * @description: MQServer
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class MQServer extends XiMQThread {

    protected NetServerFactory serverFactory;
    protected PersistentManager pm;
    protected Cache cache;
    public Configuration configuration;
    volatile boolean running = true;
    protected Processor firstProcessor;
    protected ReportBroker report;
    private Map<String, ProducerConfig> producerConfigMap = new HashMap<>();
    private Map<String, ConsumerConfig> consumerConfigMap = new HashMap<>();

    public MQServer() {
        super("MQServer");
    }

    @Override
    public synchronized void start() {
        registerBroker();
        serverFactory.start();
        super.start();
    }

    private void registerBroker() {
        report.reportBroker();
    }

    @Override
    public void run() {
        System.out.println("Starting quorum peer");
        try {
            setupRequestProcessors();
            CountDownLatch count = new CountDownLatch(1);
            count.await();
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            System.out.println("QuorumPeer main thread exited");
        }
    }

    protected void setupRequestProcessors() {
        Processor finalProcessor = new FinalProcessor(this);
        Processor persistentProcesser = new PersistentProcesser(this, finalProcessor);
        Processor syncProcesser = new SyncProcesser(this, persistentProcesser);
        firstProcessor = new PrepProcessor(this, syncProcesser);
        ((PersistentProcesser) persistentProcesser).start();
        ((SyncProcesser) syncProcesser).start();
        ((PrepProcessor) firstProcessor).start();
    }

    public void processConnectRequest(NetServer net) throws IOException {
        try {
            firstProcessor.process(net);
        } catch (Processor.ProcessorException e) {
            System.out.println("Unable to process request:" + e.getMessage());
            e.printStackTrace();
        }
    }

    public NetServerFactory getNetServerFactory() {
        return serverFactory;
    }

    public void setNetServerFactory(NetServerFactory serverNetFactory) {
        this.serverFactory = serverNetFactory;
    }

    public PersistentManager getPersistentManager() {
        return pm;
    }

    public void setPersistentManager(PersistentManager pm) {
        this.pm = pm;
    }

    public Cache getCache() {
        return cache;
    }

    public void setCache(Cache cache) {
        this.cache = cache;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public ReportBroker getReport() {
        return report;
    }

    public void setReport(ReportBroker report) {
        this.report = report;
    }

    public Map<String, ProducerConfig> getProducerConfigMap() {
        return producerConfigMap;
    }

    public Map<String, ConsumerConfig> getConsumerConfigMap() {
        return consumerConfigMap;
    }
}
