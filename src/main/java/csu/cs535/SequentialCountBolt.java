package csu.cs535;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SequentialCountBolt extends BaseRichBolt {
    ConcurrentHashMap<String, ArrayList<Long>> count_structure = new ConcurrentHashMap<>();
    long current = 1;
    OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        ArrayList<Long> counts;
        ArrayList<String> bucket = (ArrayList<String>)tuple.getValue(0);
        for (int i = 0; i < bucket.size(); i++) {
            String hashtag = bucket.get(i);
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
        }

        // Prune the data structure
        for (Map.Entry<String, ArrayList<Long>> entry : this.count_structure.entrySet()) {
            final String hashtag = entry.getKey();
            ArrayList<Long> freq_delta = entry.getValue();
            final long sum = freq_delta.get(0) + freq_delta.get(1);

            if (sum <= this.current) {
                this.count_structure.remove(hashtag);
            }
            else {
                collector.emit(new Values(hashtag, freq_delta.get(0)));
            }
        }

        this.current++;
        this.collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag", "count"));
    }
}
