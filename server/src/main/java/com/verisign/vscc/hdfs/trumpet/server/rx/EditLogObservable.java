package com.verisign.vscc.hdfs.trumpet.server.rx;

import com.verisign.vscc.hdfs.trumpet.dto.EventAndTxId;
import com.verisign.vscc.hdfs.trumpet.server.metrics.Metrics;
import org.apache.commons.beanutils.BeanMap;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.InotifyCompat;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.tools.offlineEditsViewer.OfflineEditsBinaryLoaderVisibilityWrapper;
import org.apache.hadoop.hdfs.tools.offlineEditsViewer.OfflineEditsViewer;
import org.apache.hadoop.hdfs.tools.offlineEditsViewer.OfflineEditsVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by bperroud on 07-Apr-15.
 */
public class EditLogObservable implements Observable.OnSubscribe<Map<String, Object>>, OfflineEditsVisitor {

    private static Logger LOG = LoggerFactory.getLogger(EditLogObservable.class);

    private final File editsLogFile;
    private final long startTxId;

    private final AtomicReference<Subscriber<? super Map<String, Object>>> subscriberRef = new AtomicReference<>(null);

    private final EditLogFileInputStream elis;
    private final OfflineEditsBinaryLoaderVisibilityWrapper loader;

    private long totalTxCounter = 0L;
    private long sentTxCounter = 0L;
    private long lastDecodedTxId = -1L;

    public EditLogObservable(File editsLogFile, long startTxId) {

        this.editsLogFile = editsLogFile;
        this.startTxId = startTxId;

        EditLogFileInputStream tmpElis = null;
        OfflineEditsBinaryLoaderVisibilityWrapper tmpLoader = null;

        try {
            tmpElis = new EditLogFileInputStream(editsLogFile, startTxId, HdfsServerConstants.INVALID_TXID, false);
            tmpLoader = new OfflineEditsBinaryLoaderVisibilityWrapper(this, tmpElis, new OfflineEditsViewer.Flags());
        } finally {
            if (tmpLoader == null && tmpElis != null) {
                IOUtils.closeQuietly(tmpElis);
            }
        }
        this.loader = tmpLoader;
        this.elis = tmpElis;
    }

    @Override
    public void call(Subscriber<? super Map<String, Object>> observer) {
        final Subscriber<? super Map<String, Object>> internalObserver = observer;

        if (!subscriberRef.compareAndSet(null, observer)) {
            throw new IllegalStateException("This Observable accept only one subscriber.");
        }

        LOG.debug("Start feeding observer {} with editlog {} starting at txId {}", observer, editsLogFile, startTxId);

        try {
            loader.loadEdits();
        } catch (IOException e) {
            // the exception is passed to the observer, no need to log it seriously here.
            LOG.trace("Got IOException while loading the edits.", e);

            IOUtils.closeQuietly(elis);

            if (!internalObserver.isUnsubscribed()) {
                internalObserver.onError(e);
            }
        } finally {
            LOG.debug("Processed {} tx and streamed {} events to the observer. Started at {} to last decoded txId {}", totalTxCounter, sentTxCounter, startTxId, lastDecodedTxId);
        }

    }

    @Override
    public void start(int i) throws IOException {
        subscriberRef.get().onStart();
    }

    @Override
    public void close(Throwable throwable) throws IOException {

        IOUtils.closeQuietly(elis);

        if (!subscriberRef.get().isUnsubscribed()) {
            if (throwable != null) {
                subscriberRef.get().onError(throwable);
            } else {
                subscriberRef.get().onCompleted();
            }
        }
    }

    @Override
    public void visitOp(FSEditLogOp fsEditLogOp) throws IOException {
        long txId = fsEditLogOp.getTransactionId();

        lastDecodedTxId = txId;

        if (txId < startTxId) {
            return;
        }

        Metrics.allTransaction().mark();

        Event[] events = InotifyCompat.getEvents(fsEditLogOp);

        if (subscriberRef.get().isUnsubscribed()) {
            return;
        }

        // increment counter
        totalTxCounter++;

        if (events == null) {

            // This is not a org.apache.hadoop.hdfs.inotify.Event object.
            // It is still sent to the observer to increment the
            // properly the lastPublishedTxId counter.

            subscriberRef.get().onNext(Collections.singletonMap(EventAndTxId.FIELD_TXID, (Object) Long.valueOf(txId)));

        } else {

            sentTxCounter++;

            for (Event e : events) {

                Metrics.inotifyTransaction().mark();

                // Note: org.apache.hadoop.hdfs.inotify does not have a stable API yet,
                // so using hash map here and a compat wrapper be be able to
                // be version agnostic.

                final Map<String, Object> introspected = new HashMap(new BeanMap(e));
                introspected.put(EventAndTxId.FIELD_TXID, txId);

                subscriberRef.get().onNext(introspected);
            }
        }
    }
}
