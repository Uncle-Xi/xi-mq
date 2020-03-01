package com.ximq.common.config;

import com.xicp.WatchedEvent;
import com.xicp.Watcher;
import com.xicp.XiCP;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @description: Lock
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class Lock implements Watcher {

    private static final String DEFUALT_LOCK = "/ximq/locks";
    private XiCP xiCP;

    public Lock(String connectString) throws IOException {
        this.xiCP = new XiCP(connectString, this);
    }

    public boolean lock(String lock, long timeout) {
        if (lock == null) {
            lock = DEFUALT_LOCK;
        }
        if (timeout < 0) {
            throw new RuntimeException("timeout must greater than or equals zero.");
        }
        if (lock.indexOf("/") == -1) {
            lock = "/" + lock;
        }
        if (lock.lastIndexOf("/") == lock.length() - 1) {
            lock = lock.substring(0, lock.length() - 1);
        }
        try {
            if (xiCP.exists(lock, true)) {
                Thread.sleep(timeout);
                if (!xiCP.exists(lock, true)) {
                    xiCP.create(lock, lock.getBytes(), true, false);
                    return true;
                }
            } else {
                xiCP.create(lock, lock.getBytes(), true, false);
                return true;
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public boolean unLock(String lock) {
        if (lock == null) {
            lock = DEFUALT_LOCK;
        }
        if (lock.indexOf("/") == -1) {
            lock = "/" + lock;
        }
        if (lock.lastIndexOf("/") == lock.length() - 1) {
            lock = lock.substring(0, lock.length() - 1);
        }
        try {
            xiCP.delete(lock);
            return true;
        } catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        // TODO
    }
}
