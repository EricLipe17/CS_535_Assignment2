package csu.cs535;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class LogBolt extends BaseBasicBolt {
    // TODO: add data strucuture to persist most popular hashtags
    FileWriter fw = new FileWriter("hashtag_counts.log", true);
    BufferedWriter bw = new BufferedWriter(fw);

    public LogBolt() throws IOException {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // TODO: implement logging every 10 seconds
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag", "count_and_delta"));
    }
}
