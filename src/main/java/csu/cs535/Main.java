package csu.cs535;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.InputDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Objects;

public class Main {
    public static void main(String[] args) {
        int numCountBolts = 1;
        if (args.length > 0 && Objects.equals(args[0], "-p")) {
            numCountBolts = 4;
        }
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("FakeTwitterSpout", new TwitterSampleSpout());
        for (int i = 0; i < numCountBolts; i++) {
            builder.setBolt("CountBolt" + i, new CountBolt(20), 1).fieldsGrouping("FakeTwitterSpout", new Fields("hashtag"));
        }
        InputDeclarer<BoltDeclarer> declarer = builder.setBolt("LogBolt", new LogBolt(), 1).shuffleGrouping("CountBolt0");;
        for (int i = 1; i < numCountBolts; i++) {
            declarer = declarer.shuffleGrouping("CountBolt" + i);
        }

        Config conf = new Config();
        conf.setDebug(true);
        String topoName = "Hashtags";
        conf.setNumWorkers(numCountBolts);
        conf.setMessageTimeoutSecs(300);
        try {
            StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
            throw new RuntimeException(e);
        }
    }
}
