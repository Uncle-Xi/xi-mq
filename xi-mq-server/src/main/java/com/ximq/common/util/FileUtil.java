package com.ximq.common.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

/**
 * @description: FileUtil
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class FileUtil {

    public static File createDir(String path) {
        File dir = new File(path);
        if (!dir.exists()) {
            //System.out.println("文件夹不存在，立即创建 > " + dir.getAbsolutePath());
            dir.mkdirs();
        }
        return dir;
    }

    public static File createFile(String path, String fileName) {
        File file = new File(path, fileName);
        try {
            if (!file.exists()) {
                //System.out.println("文件不存在，立即创建 > " + file.getAbsolutePath());
                file.createNewFile();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return file;
    }

    public static File createFile(String absolutePath) {
        File file = new File(absolutePath);
        try {
            if (!file.exists()) {
                //System.out.println("文件不存在，立即创建 > " + file.getAbsolutePath());
                file.createNewFile();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return file;
    }

    public static void writeContent(File logs, String log) throws IOException {
        try {
            FileOutputStream out = new FileOutputStream(logs, true);
            FileChannel channel = out.getChannel();
            byte[] bytes = (log + "\n").getBytes(Charset.forName("UTF-8"));
            ByteBuffer bb = ByteBuffer.allocateDirect(bytes.length);
            bb.put(bytes);
            bb.flip();
            channel.write(bb);
            channel.close();
            if (out != null) {
                out.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static long fileLineCnt(File file) {
        try {
            FileReader input = new FileReader(file);
            LineNumberReader count = new LineNumberReader(input);
            while (count.skip(Long.MAX_VALUE) > 0) {
            }
            return count.getLineNumber() + 1;
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    public static String getFirstLine(File lastFile) {
        RandomAccessFile r = null;
        try {
            r = new RandomAccessFile(lastFile, "r");
            return r.readLine();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (r != null) {
                try {
                    r.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    public static String getLastLine(File file) {
        if (file == null) {
            System.err.println("[getLastLine] [file == null]..");
            return null;
        }
        if (!file.isFile()) {
            System.err.println("[getLastLine] [file.isFile()].." + file.isFile());
            return null;
        }
        RandomAccessFile r = null;
        try {
            r = new RandomAccessFile(file, "r");
            long len = r.length();
            if (len == 0L) {
                return "";
            } else {
                long pos = len - 1;
                while (pos > 0) {
                    pos--;
                    r.seek(pos);
                    if (r.readByte() == '\n') {
                        break;
                    }
                }
                if (pos == 0) {
                    r.seek(0);
                }
                byte[] bytes = new byte[(int) (len - pos)];
                r.read(bytes);
                return new String(bytes, "UTF-8");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (r != null) {
                try {
                    r.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
}
