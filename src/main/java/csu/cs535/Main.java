package csu.cs535;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class Main {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("FakeTwitterSpout", new TwitterSampleSpout());
        builder.setBolt("BucketBolt", new BucketBolt(100), 1).shuffleGrouping("FakeTwitterSpout");
        builder.setBolt("CountBolt", new SequentialCountBolt(), 1).shuffleGrouping("BucketBolt");
        builder.setBolt("LogBolt", new LogBolt(), 1).shuffleGrouping("CountBolt");

        Config conf = new Config();
        conf.setDebug(true);
        String topoName = "Sequential Count Topology";
        conf.setNumWorkers(1);
        try {
            StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
            throw new RuntimeException(e);
        }
    }
}
