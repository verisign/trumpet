package com.verisign.vscc.hdfs.trumpet.server.exception;

/**
 * Created by bperroud on 09.04.15.
 */
public class EditLogObservableException extends IllegalStateException {
    public EditLogObservableException(String m, Throwable t) {
        super(m, t);
    }
}
