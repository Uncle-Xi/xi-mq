package com.ximq.common.persistent;

import com.ximq.common.message.Request;
import com.ximq.common.util.FileUtil;

import java.io.*;

/**
 * @description: ReadLog
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class ReadLog {

    protected static final String SPLIT = ";";
    protected static final String FILE_SUFFIX = ".log";
    private final PersistentManager pm;

    public ReadLog(PersistentManager pm) {
        this.pm = pm;
    }

    public Object readLog(Request request) {
        Object object = null;
        try {
            File offsetFile = this.createOffsetFile(request);
            String ll = FileUtil.getLastLine(offsetFile);
            ll = ll == null ? "-1" : ll;
            ll = ll.replaceAll("\\s*|\r|\n|\t", "");
            ll = ll.equals("")? "-1" : ll;
            long lastOffset = Long.valueOf(ll.trim()) + 1;
            //object = pm.cache.get(lastOffset);
            object = this.doReadLog(this.indexLogOffset(request.getPartition(), lastOffset), lastOffset);
            if (object != null) {
                //System.out.println("读取到了数据，更新 offset ...");
                this.updateOffsetLog(request, lastOffset);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return object;
    }

    protected File indexLogOffset(String partition, long offset) {
        String partitionDir  = pm.logDirs + File.separator + partition;
        String indexFilePath = partitionDir + File.separator + "index.log";
        try {
            //System.out.println("[indexLogOffset] [indexFilePath] -> " + indexFilePath);
            File indexFile = FileUtil.createFile(indexFilePath);
            File logFile = null;
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(indexFile), "UTF-8"));
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] indexLine = line.split(" ");
                long start = Long.valueOf(indexLine[0]);
                long end = Long.valueOf(indexLine[1]);
                if (start <= offset && offset <= end) {
                    logFile =  new File(indexLine[2]);
                    break;
                }
            }
            br.close();
            if (logFile != null && logFile.isFile()) {
                //System.out.println("[indexLogOffset] 从索引文件中找到了日志文件...");
                return logFile;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        //System.out.println("[indexLogOffset] [从 last log file 读取数据]...");
        return pm.findLastFile(new File(partitionDir));
    }

    protected Object doReadLog(File logFile, long lastOffset) {
        if (logFile == null) {
            //System.out.println("[doReadLog] [logFile] == null..");
            return lastOffset;
        }
        if (!logFile.isFile()) {
            //System.err.println("[doReadLog] [isFile] -> " + logFile.isFile());
            return null;
        }
        //System.out.println("[doReadLog] [logFile] -> " + logFile.getAbsolutePath());
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(logFile), "UTF-8"));
            String line = null;
            while ((line = br.readLine()) != null) {
                long offset = Long.valueOf(line.substring(0, line.indexOf(SPLIT)));
                if (lastOffset == offset) {
                    line = line.substring((offset + SPLIT).length());
                    break;
                }
            }
            br.close();
            //System.out.println("[doReadLog] [readLine] -> " + line);
            return line;
        } catch (Exception e) {
            System.err.println("read errors :" + e);
        }
        return null;
    }


    protected void updateOffsetLog(Request request, long lastOffset) {
        File offsetFile = createOffsetFile(request);
        if (offsetFile == null) {
            System.out.println("updateOffsetLog offset == null..");
            return;
        }
        try {
            FileUtil.writeContent(offsetFile, "" + lastOffset);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected File createOffsetFile(Request request) {
        try {
            String partitionDir   = pm.logDirs + File.separator + request.getPartition();
            String groupDirPath   = partitionDir + File.separator + request.getGroupId();
            String offsetFileName = request.getPartition() + "-" + request.getGroupId() + "-offset.log";
            File offsetFile = new File(groupDirPath, offsetFileName);
            if (offsetFile.exists() && offsetFile.isFile()) {
                //System.out.println("offsetFile 已经存在，直接返回文件对象即可..." + offsetFile.getAbsolutePath());
                return offsetFile;
            }
            File   partitionFile  = FileUtil.createDir(partitionDir);
            File   groupDir       = FileUtil.createDir(groupDirPath);
            //System.out.println("offsetFileName -> " + offsetFileName);
            //System.out.println("offsetFilePath -> " + groupDirPath);
            //System.out.println("groupFile -> " + groupDir == null ? null : groupDir.getAbsolutePath());
            return FileUtil.createFile(groupDirPath, offsetFileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
