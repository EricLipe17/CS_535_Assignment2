package csu.cs535;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Map;

public class BucketBolt extends BaseRichBolt {
    ArrayList<String> hashtags;
    int numHashtags;
    private OutputCollector collector;

    public BucketBolt(int numHashtags) {
        this.numHashtags = numHashtags;
        this.hashtags = new ArrayList<>(this.numHashtags);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtags"));
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        this.hashtags.add(tuple.getString(0));

        if (this.hashtags.size() == this.numHashtags) {
            this.collector.emit(new Values(this.hashtags));
            this.hashtags.clear();
        }
        this.collector.ack(tuple);
    }
}
