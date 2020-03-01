package com.ximq.common;

import com.ximq.common.message.Record;

public interface Processor {

    class ProcessorException extends Exception {
        public ProcessorException(String msg, Throwable t) {
            super(msg, t);
        }
    }

    void process(Record record) throws ProcessorException;
}
