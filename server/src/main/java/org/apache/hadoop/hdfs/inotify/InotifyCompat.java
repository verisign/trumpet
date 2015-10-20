package org.apache.hadoop.hdfs.inotify;

import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.InotifyFSEditLogOpTranslator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * https://issues.apache.org/jira/browse/HDFS-7446 released in 2.7 change the API.
 * <p/>
 * A new EventBatch wrapping the Event[] has been added.
 * <p/>
 * This class tries to keep the project version agnostic.
 */
public class InotifyCompat {

    private static final boolean useV27;

    private static final Method getEventsFromEventBatchCls;
    private static final Method translateFromInotifyFSEditLogOpTranslator;

    static {
        Method tmpGetEventsFromEventBatchCls = null;
        Method tmpTranslateFromInotifyFSEditLogOpTranslator = null;

        try {
            Class<?> inotifyFSEditLogOpTranslatorCls = InotifyFSEditLogOpTranslator.class;
            tmpTranslateFromInotifyFSEditLogOpTranslator = inotifyFSEditLogOpTranslatorCls.getMethod("translate", FSEditLogOp.class);
        } catch (NoSuchMethodException e) {
            throw new Error("Initialization exception, please correct your classpath", e);
        }
        translateFromInotifyFSEditLogOpTranslator = tmpTranslateFromInotifyFSEditLogOpTranslator;

        boolean v27 = true;
        try {
            Class<?> eventBatchCls = Class.forName("org.apache.hadoop.hdfs.inotify.EventBatch");
            tmpGetEventsFromEventBatchCls = eventBatchCls.getMethod("getEvents");
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            v27 = false;
        }
        useV27 = v27;
        getEventsFromEventBatchCls = tmpGetEventsFromEventBatchCls;
    }

    public static Event[] getEvents(FSEditLogOp op) {

        if (op == null) {
            return null;
        }

        try {
            Object translated = translateFromInotifyFSEditLogOpTranslator.invoke(null, op);
            return toEvent(translated);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("Can't invoke method " + getEventsFromEventBatchCls.getName(), e);
        }
    }

    private static Event[] toEvent(Object o) {

        if (o == null) {
            return null;
        }

        if (useV27) {
            try {
                return (Event[]) getEventsFromEventBatchCls.invoke(o);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalArgumentException("Can't invoke method " + getEventsFromEventBatchCls.getName(), e);
            }
        } else {
            return (Event[]) o;
        }
    }
}
