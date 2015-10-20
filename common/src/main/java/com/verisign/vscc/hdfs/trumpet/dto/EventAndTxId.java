package com.verisign.vscc.hdfs.trumpet.dto;

/**
 * Centralized place to store DTO-related stuff.
 * <p/>
 * Note: I'm too lazy to define proper Pojo as the JSON representation is the same anyway ;)
 * The main data structure used is a Map
 * <p/>
 * DTO from org.apache.hadoop.hdfs.inotify
 * can and will be used on the client side.
 */
public interface EventAndTxId {
    public static final String FIELD_TXID = "txId";
    public static final String FIELD_EVENTTYPE = "eventType";
    public static final String FIELD_PATH = "path";
    public static final String FIELD_SRCPATH = "srcPath";
    public static final String FIELD_DSTPATH = "dstPath";
}
