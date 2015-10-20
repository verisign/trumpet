package com.verisign.vscc.hdfs.trumpet.client.example;

import com.google.common.collect.Sets;
import com.verisign.vscc.hdfs.trumpet.AbstractAppLauncher;
import com.verisign.vscc.hdfs.trumpet.client.TrumpetEventStreamer;
import com.verisign.vscc.hdfs.trumpet.dto.EventAndTxId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import rx.*;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.*;

public class TestApp extends AbstractAppLauncher {

    private final Set<String> EVENT_TYPES = Sets.newHashSet("CREATE");

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TestApp(), args);
        System.exit(res);
    }

    @Override
    protected int internalRun() throws Exception {

        TrumpetEventStreamer trumpetEventStreamer = new TrumpetEventStreamer(getCuratorFrameworkKafka(), getTopic());

        rx.Observable.from(trumpetEventStreamer)

                    /*
                     * Keep only events of type CREATE
                     */
                .filter(new Func1<Map<String, Object>, Boolean>() {
                    @Override
                    public Boolean call(Map<String, Object> o) {
                        final String eventType = o.get(EventAndTxId.FIELD_EVENTTYPE).toString();
                        return EVENT_TYPES.contains(eventType);
                    }
                })

                    /*
                     * Keep only events which filename (path ends with) _SUCCESS
                     */
                .filter(new Func1<Map<String, Object>, Boolean>() {
                    @Override
                    public Boolean call(Map<String, Object> o) {
                        Object txId = o.get(EventAndTxId.FIELD_TXID);
                        String path = o.get(EventAndTxId.FIELD_PATH).toString();
                        return path.endsWith("/_SUCCESS");
                    }
                })

                    /*
                     * Just ensure this application does terminate one day
                     */
                .limit(10)

                    /*
                     * Do something useful with this event.
                     * Only logging here, but a distcp in the parent folder could be triggered for instance.
                     */
                .subscribe(new Action1<Map<String, Object>>() {
                    @Override
                    public void call(Map<String, Object> o) {
                        Object txId = o.get(EventAndTxId.FIELD_TXID);
                        String path = o.get(EventAndTxId.FIELD_PATH).toString();

                        LOG.debug("Woot, I got a new _SUCCESS file {} @{}", path, txId);
                        LOG.debug("I can do something with it now, for instance starting distcp on the parent folder:");
                        String parentFolder = new Path(path).getParent().toUri().getPath();
                        LOG.info("$ hadoop distcp {} hdfs://another-hdfs{}", parentFolder, parentFolder);
                    }
                });

        return ReturnCode.ALL_GOOD;
    }

}
