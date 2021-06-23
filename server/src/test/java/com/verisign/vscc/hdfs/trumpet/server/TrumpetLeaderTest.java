package com.verisign.vscc.hdfs.trumpet.server;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Assert;
import org.junit.Test;

import com.verisign.vscc.hdfs.trumpet.dto.EventAndTxId;
import com.verisign.vscc.hdfs.trumpet.server.editlog.EditLogDir;
import com.verisign.vscc.hdfs.trumpet.server.rx.EditLogObservable;
import com.verisign.vscc.hdfs.trumpet.server.rx.ProducerSubscriber;

import junit.framework.TestCase;
import rx.Subscriber;

public class TrumpetLeaderTest extends TestCase {

    private  static final Random R = new Random();

    long TX_ID_1 = 1234L;
    long TX_ID_2 = 2345L;
    long TX_ID_3 = 3456L;
    long TX_ID_4 = 4567L;

    String topic = "hdfs_trumpet_" + R.nextInt(Integer.MAX_VALUE);

    @Test
    public void testNoFileFound() throws Exception {


        final CuratorFramework curatorFramework = mock(CuratorFramework.class);
        EditLogDir editLogDir = mock(EditLogDir.class);

        DistributedFileSystem dfs = mock(DistributedFileSystem.class);

        final TrumpetLeader trumpetLeader = spy(new TrumpetLeader(curatorFramework, dfs, topic, editLogDir));

        Producer producer = mock(Producer.class);
        EditLogObservable editLogObservable = mock(EditLogObservable.class);
        ProducerSubscriber producerSubscriber = mock(ProducerSubscriber.class);

        doReturn(producer).when(trumpetLeader)
                .getProducer(eq(curatorFramework));

        doReturn(editLogObservable).when(trumpetLeader)
                .getEditLogObservable(any(File.class), eq(TX_ID_1));

        doReturn(producerSubscriber).when(trumpetLeader)
                .getProducerSubscriber(eq(topic), eq(producer), any(AtomicLong.class));

        doReturn(new AtomicLong())
                .when(trumpetLeader).getAtomicLong(eq(topic), anyBoolean(), eq(curatorFramework));

        ExecutorService executorService = Executors.newCachedThreadPool();
        final CountDownLatch latch = new CountDownLatch(1);

        Future<?> submit = executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    trumpetLeader.takeLeadership(curatorFramework);
                } catch (CancelLeadershipException e) {
                    latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail();
                }
            }
        });

        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testStreamingEvents() throws Exception {

        final CuratorFramework curatorFramework = mock(CuratorFramework.class);
        EditLogDir editLogDir = mock(EditLogDir.class);

        DistributedFileSystem dfs = mock(DistributedFileSystem.class);

        final TrumpetLeader trumpetLeader = spy(new TrumpetLeader(curatorFramework, dfs, topic, editLogDir, 200));

        Producer producer = mock(Producer.class);
        File file1 = mock(File.class);
        doReturn(file1).when(editLogDir).searchBestMatchingSegment(eq(TX_ID_1 + 1));

        EditLogObservable editLogObservable1 = new EditLogObservable(file1, TX_ID_1) {
            @Override
            public void call(Subscriber<? super Map<String, Object>> observer) {
                for (int i = 1; i < 5; i++) {
                    final int j = i;
                    Map<String, Object> o1 = new HashMap<String, Object>() {
                        {
                            put(EventAndTxId.FIELD_EVENTTYPE, "CREATE");
                            put(EventAndTxId.FIELD_TXID, (Object) (TX_ID_1 + j));
                        }
                    };
                    observer.onNext(o1);
                }
                Map<String, Object> o1 = new HashMap<String, Object>() {
                    {
                        put(EventAndTxId.FIELD_EVENTTYPE, "CREATE");
                        put(EventAndTxId.FIELD_TXID, (Object) (TX_ID_2));
                    }
                };
                observer.onNext(o1);
            }
        };

        File file2 = mock(File.class);
        doReturn(file2).when(editLogDir).searchBestMatchingSegment(TX_ID_2 + 1);
        EditLogObservable editLogObservable2 = new EditLogObservable(file2, TX_ID_2 + 1) {
            @Override
            public void call(Subscriber<? super Map<String, Object>> observer) {
                for (int i = 1; i < 5; i++) {
                    final int j = i;
                    Map<String, Object> o1 = new HashMap<String, Object>() {
                        {
                            put(EventAndTxId.FIELD_EVENTTYPE, "CREATE");
                            put(EventAndTxId.FIELD_TXID, (Object) (TX_ID_2 + j));
                        }
                    };
                    observer.onNext(o1);
                }
                Map<String, Object> o1 = new HashMap<String, Object>() {
                    {
                        put(EventAndTxId.FIELD_EVENTTYPE, "CREATE");
                        put(EventAndTxId.FIELD_TXID, (Object) (TX_ID_3));
                    }
                };
                observer.onNext(o1);
            }
        };

        File file3 = mock(File.class);
        doReturn(file3).when(editLogDir).searchBestMatchingSegment(TX_ID_3 + 1);
        EditLogObservable editLogObservable3 = new EditLogObservable(file3, TX_ID_3 + 1) {
            @Override
            public void call(Subscriber<? super Map<String, Object>> observer) {
                throw new RuntimeException("Dummy RuntimeException");
            }
        };
        EditLogObservable editLogObservable4 = new EditLogObservable(file3, TX_ID_3 + 1) {
            @Override
            public void call(Subscriber<? super Map<String, Object>> observer) {
                throw new CancelLeadershipException("Dummy CancelLeadershipException");
            }
        };

        AtomicLong atomicLong = new AtomicLong(TX_ID_1);

        ProducerSubscriber producerSubscriber = spy(new ProducerSubscriber(topic, producer, atomicLong));

        doReturn(producer).when(trumpetLeader)
                .getProducer(eq(curatorFramework));

        doReturn(editLogObservable1).when(trumpetLeader)
                .getEditLogObservable(eq(file1), eq(TX_ID_1 + 1));
        doReturn(editLogObservable2).when(trumpetLeader)
                .getEditLogObservable(eq(file2), eq(TX_ID_2 + 1));
        doReturn(editLogObservable3).doReturn(null)
                .when(trumpetLeader)
                .getEditLogObservable(eq(file3), eq(TX_ID_3 + 1));

        doReturn(producerSubscriber).when(trumpetLeader)
                .getProducerSubscriber(eq(topic), eq(producer), any(AtomicLong.class));
        doCallRealMethod().when(producerSubscriber).onNext(anyMap());

        doReturn(atomicLong).when(trumpetLeader)
                .getAtomicLong(anyString(), anyBoolean(), eq(curatorFramework));

        ExecutorService executorService = Executors.newCachedThreadPool();
        final CountDownLatch latch = new CountDownLatch(1);

        Future<?> submit = executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    trumpetLeader.takeLeadership(curatorFramework);
                } catch (CancelLeadershipException e) {
                    latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail();
                }
            }
        });

        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

}