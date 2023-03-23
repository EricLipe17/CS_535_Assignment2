package csu.cs535;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class BucketBolt extends BaseBasicBolt {
    ArrayList<String> hashtags;
    int numHashtags;

    public BucketBolt(int numHashtags) {
        this.numHashtags = numHashtags;
        this.hashtags = new ArrayList<>(this.numHashtags);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        this.hashtags.add(tuple.getString(0));

        if (this.hashtags.size() == this.numHashtags) {
            collector.emit(new Values(this.hashtags));
            this.hashtags.clear();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtags"));
    }
}
