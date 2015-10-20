package com.verisign.vscc.hdfs.trumpet.server.exception;

/**
 * Created by bperroud on 09.04.15.
 */
public class ProducerSubscriberException extends IllegalStateException {
    public ProducerSubscriberException(String m, Throwable t) {
        super(m, t);
    }
}
