package com.ximq.common.persistent;

import com.ximq.common.message.Request;
import com.ximq.common.util.FileUtil;

import java.io.File;
import java.io.IOException;

/**
 * @description: WriteLog
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class WriteLog {

    protected static final String SPLIT = ";";
    protected static final String FILE_SUFFIX = ".log";
    private final PersistentManager pm;

    public WriteLog(PersistentManager pm) {
        this.pm = pm;
    }

    public void appendLog(Request request) {
        try {
            if (null == request.getData() || "".equals(request.getData())) {
                System.out.println("空数据，直接结束...");
                return;
            }
            String partitionDir    = pm.logDirs + File.separator + request.getPartition();
            File partition         = FileUtil.createDir(partitionDir);
            String logFileName     = "0" + FILE_SUFFIX;
            File lastFile          = pm.findLastFile(partition);
            File logsFile          = null;
            long offset = 0;
            boolean creatIndexFile = false;
            boolean creatLogsFile  = false;
            if (lastFile != null) {
                String ll         = FileUtil.getLastLine(lastFile);
                ll = ll == null? "-1" : ll.trim();
                ll = ll.equals("")? "-1" : ll.equals("null")? "-1" : ll;
                ll = ll.contains(SPLIT)? ll.substring(0, ll.indexOf(SPLIT)) : ll;
                if (ll.equals("-1")) {
                    creatLogsFile = true;
                } else {
                    logsFile = lastFile;
                }
                offset = Long.valueOf(ll) + 1;
                if (lastFile.length() >= pm.config.getLogSegmentBytes()) {
                    logFileName = offset + FILE_SUFFIX;
                    creatIndexFile = true;
                    creatLogsFile  = true;
                } else {
                    logFileName = lastFile.getName();
                }
                if (creatLogsFile) {
                    logsFile = FileUtil.createFile(partitionDir, logFileName);
                }
            } else {
                logsFile = FileUtil.createFile(partitionDir, logFileName);
            }
            if (creatIndexFile) {
                this.createIndexFile(lastFile, request.getPartition());
            }
            //pm.cache.put(offset, request.getData());
            FileUtil.writeContent(logsFile, offset + SPLIT + request.getData());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    protected void createIndexFile(File lastFile, String partition) {
        try {
            String indexFilePath = pm.logDirs + File.separator + partition + File.separator + "index.log";
            File indexFile = FileUtil.createFile(indexFilePath);
            if (lastFile == null) {
                System.out.println("createIndexFile lastFile == null...");
                return;
            }
            String lastLine = FileUtil.getLastLine(lastFile);
            String firstLine = FileUtil.getFirstLine(lastFile);
            lastLine = lastLine == null? ";" : lastLine.trim();
            firstLine = firstLine == null? ";" : firstLine.trim();

            long startOffset = Long.valueOf(firstLine.substring(0, firstLine.indexOf(SPLIT)));
            long endOffset = Long.valueOf(lastLine.substring(0, lastLine.indexOf(SPLIT)));

            String indexALine = startOffset + " " + endOffset + " " + lastFile.getAbsolutePath();
            FileUtil.writeContent(indexFile, indexALine);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
