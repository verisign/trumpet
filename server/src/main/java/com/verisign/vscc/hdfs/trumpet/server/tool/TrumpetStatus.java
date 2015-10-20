package com.verisign.vscc.hdfs.trumpet.server.tool;

import com.google.common.net.InetAddresses;
import com.verisign.vscc.hdfs.trumpet.AbstractAppLauncher;
import com.verisign.vscc.hdfs.trumpet.server.TrumpetServer;
import com.verisign.vscc.hdfs.trumpet.server.TrumpetServerCLI;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.net.InetAddress;

public class TrumpetStatus extends AbstractAppLauncher {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TrumpetStatus(), args);
        System.exit(res);
    }

    @Override
    protected int internalRun() throws Exception {

        LeaderSelector leaderSelector = new LeaderSelector(getCuratorFrameworkUser(), TrumpetServer.zkLeaderElectionName(getTopic()), new LeaderSelectorListenerAdapter() {
            @Override
            public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
                // noop, not aimed at taking leadership, just getting participants
            }
        });

        boolean hasParticipants = false;
        for (Participant p : leaderSelector.getParticipants()) {
            InetAddress inetAddress = InetAddresses.forString(p.getId());
            System.out.println(inetAddress.getHostName() + "(" + inetAddress.getHostAddress() + ") is " + (p.isLeader() ? "leader" : "participant"));
            hasParticipants = true;
        }

        if (hasParticipants) {
            return ReturnCode.ALL_GOOD;
        } else {
            return ReturnCode.GENERIC_ERROR;
        }
    }

}
