package com.ximq.common;

public interface OpCode {
    int producerConnect = 1;
    int consumerConnect = 2;
    int producerSend = 3;
    int consumerReceive = 4;
    int producerPing = 5;
    int consumerPing = 6;
    int unkonwn = 0;
    int success = 9;
}
