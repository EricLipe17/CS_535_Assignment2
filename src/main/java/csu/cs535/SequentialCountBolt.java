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

public class SequentialCountBolt extends BaseBasicBolt {
    Map<String, ArrayList<Long>> count_structure = new HashMap<>();
    long current = 1;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        ArrayList<Long> counts;
        for (int i = 0; i < tuple.size(); i++) {
            String hashtag = tuple.getString(i);
            counts = this.count_structure.get(hashtag);
            if (counts == null) {
                counts = new ArrayList<>(2);
                counts.add(0L);
                counts.add(this.current - 1L);
            }
            long count = counts.get(0);
            count++;
            counts.set(0, count);
            this.count_structure.put(hashtag, counts);

            // TODO: implement pruning piece of algorithm
            // TODO: consider implementing this as a sliding window?

            collector.emit(new Values(hashtag, counts));
        }
        this.current++;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag", "count_and_delta"));
    }
}
