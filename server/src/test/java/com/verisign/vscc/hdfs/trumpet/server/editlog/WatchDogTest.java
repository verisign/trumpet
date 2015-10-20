package com.verisign.vscc.hdfs.trumpet.server.editlog;

import com.google.common.io.Files;
import junit.framework.TestCase;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Created by bperroud on 8/15/15.
 */
public class WatchDogTest extends TestCase {

    @Test
    public void test1() throws Exception {

        long TX_ID_1 = 1234L;
        long TX_ID_2 = 2345L;
        long TX_ID_3 = 3456L;
        long TX_ID_4 = 4567L;

        DistributedFileSystem dfs = Mockito.mock(DistributedFileSystem.class);
        DFSClient client = Mockito.mock(DFSClient.class);
        ClientProtocol namenode = Mockito.mock(ClientProtocol.class);

        Mockito.doReturn(client).when(dfs).getClient();
        Mockito.doReturn(namenode).when(client).getNamenode();
        Mockito.doReturn(TX_ID_1)
                .doReturn(TX_ID_2)
                .doReturn(TX_ID_3)
                .doReturn(TX_ID_4)
                .when(namenode).getCurrentEditLogTxid();

        EditLogDir editLogDir = Mockito.mock(EditLogDir.class);


        AutoCloseable autoCloseable = Mockito.mock(AutoCloseable.class);

        WatchDog watchDog = Mockito.spy(new WatchDog(dfs, editLogDir, autoCloseable));

        Assert.assertEquals(-1L, watchDog.getLastSeenTxId());

        // First run
        watchDog.run();
        Assert.assertEquals(TX_ID_1, watchDog.getLastSeenTxId());
        Mockito.verifyZeroInteractions(editLogDir, autoCloseable);

        // Sleeping one second between the runs because file modification precision is probably seconds and not ms.
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        // Next run
        File file1 = Mockito.spy(Files.createTempDir());
        Mockito.doCallRealMethod().when(file1).lastModified();
        Mockito.doReturn(file1).when(editLogDir).searchBestMatchingSegment(TX_ID_2);

        watchDog.run();
        Assert.assertEquals(TX_ID_2, watchDog.getLastSeenTxId());
        Mockito.verify(editLogDir, Mockito.times(1)).searchBestMatchingSegment(TX_ID_2);
        Mockito.verify(autoCloseable, Mockito.times(0)).close();

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        // Next run
        File file2 = Mockito.spy(Files.createTempDir());
        Mockito.doCallRealMethod().when(file2).lastModified();
        Mockito.doReturn(file2).when(editLogDir).searchBestMatchingSegment(TX_ID_3);

        watchDog.run();
        Assert.assertEquals(TX_ID_3, watchDog.getLastSeenTxId());
        Mockito.verify(editLogDir, Mockito.times(1)).searchBestMatchingSegment(TX_ID_3);
        Mockito.verify(autoCloseable, Mockito.times(0)).close();

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        // Next run
        Mockito.doReturn(file2).when(editLogDir).searchBestMatchingSegment(TX_ID_4);

        watchDog.run();
        Assert.assertEquals(TX_ID_4, watchDog.getLastSeenTxId());
        Mockito.verify(editLogDir, Mockito.times(1)).searchBestMatchingSegment(TX_ID_4);
        Mockito.verify(autoCloseable, Mockito.times(1)).close();
    }
}