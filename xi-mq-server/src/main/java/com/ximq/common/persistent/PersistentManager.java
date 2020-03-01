package com.ximq.common.persistent;

import com.ximq.common.cache.Cache;
import com.ximq.common.config.Configuration;
import com.ximq.common.config.ConsumerConfig;
import com.ximq.common.message.Record;
import com.ximq.common.message.Request;
import com.ximq.common.util.FileUtil;
import com.ximq.server.MQServer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @description: PersistentManager
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class PersistentManager {

    protected ReadLog readLog;
    protected WriteLog writeLog;
    protected Configuration config;
    protected String logDirs;
    protected Cache cache;
    protected MQServer server;
    protected static final String SPLIT = ";";
    protected static final String FILE_SUFFIX = ".log";

    public PersistentManager(Configuration config, MQServer server) {
        this.config = config;
        this.logDirs = config.getLogDirs();
        this.server = server;
        this.cache = server.getCache();
        this.readLog = new ReadLog(this);
        this.writeLog = new WriteLog(this);
    }

    public void appendLog(Request request) {
        writeLog.appendLog(request);
    }

    public Object readLog(Request request) {
        return readLog.readLog(request);
    }

    protected File findLastFile(File file) {
        try {
            if (file == null) {
                System.out.println("[findLastFile] - [file] -> null");
                return null;
            }
            File[] files = file.listFiles();
            //System.out.println("[findLastFile] - [listFiles] -> " + (files == null ? "0" : files.length));
            long max = 0;
            File lastFile = files == null ? null : files.length > 0 ? files[0] : null;
            for (File f : files) {
                if (f.isFile() && !f.getName().contains("index")) {
                    String fileName = f.getName();
                    long millis = Long.valueOf(fileName.replaceAll(FILE_SUFFIX, ""));
                    if (millis > max) {
                        max = millis;
                        lastFile = f;
                    }
                }
            }
            //System.out.println("[findLastFile] - [lastFile] -> " + (lastFile == null ? "null" : lastFile.getAbsolutePath()));
            return lastFile;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public long lastOffset() {
        return -1;
    }


    protected synchronized long getOffset() {
        long offset = 0;
        return offset;
    }

    public static void main(String[] args) {
        //System.out.println(Long.toBinaryString((long) 1 << (Long.SIZE - 1)));
        //String fileName = "000000000000000..log";
        //fileName = fileName.replaceAll(FILE_SUFFIX, "");
        //System.out.println(fileName);
        //String ll = "offset;;;{object}";
        //ll = ll.substring(0, ll.indexOf(SPLIT));
        //System.out.println(ll);
        //String line = "999;888";
        //line = line.substring((999 + SPLIT).length());
        //System.out.println(line);
        //String fP = "D:\\Code\\JavaIDEA\\LRN\\Framework\\xi-mq\\xi-mq-server\\src\\main\\resources\\ximq-logs\\hello-0\\0.log";
        //String result = new PersistentManager().lastLine(new File(fP));
        //System.out.println(result);

        String dtr = "ok\n" + ".";
        System.out.println(dtr.replaceAll("\r|\n", ""));
    }
}
