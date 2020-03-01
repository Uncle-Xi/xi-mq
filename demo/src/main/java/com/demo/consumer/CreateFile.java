package com.demo.consumer;

import java.io.File;

/**
 * @description: create file
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class CreateFile {

    public static void main(String[] args) {
        try {
            String filePath = "D:\\Code\\xi-mq\\xi-mq-server\\src\\main\\resources\\ximq-logs\\127.0.0.1:9092";
            File f = new File(filePath);
            if (!f.exists()) {
                f.createNewFile();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
