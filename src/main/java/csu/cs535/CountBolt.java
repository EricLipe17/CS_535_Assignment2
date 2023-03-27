package csu.cs535;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CountBolt extends BaseRichBolt {
    ConcurrentHashMap<String, ArrayList<Long>> count_structure;
    ArrayList<String> hashtags;
    int numHashtags;
    long current = 1;
    OutputCollector collector;

    public CountBolt(int numHashtags) {
        this.numHashtags = numHashtags;
        this.hashtags = new ArrayList<>(this.numHashtags);
        this.count_structure = new ConcurrentHashMap<>();
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        this.collector.ack(tuple);

        // Collect hashtag and execute lossy count algorithm if our bucket is full
        this.hashtags.add(tuple.getString(0));
        if (this.hashtags.size() == this.numHashtags) {
            // Update the counts
            ArrayList<Long> counts;
            for (int i = 0; i < this.hashtags.size(); i++) {
                String hashtag = this.hashtags.get(i);
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

            // Clean up the bucket
            this.hashtags.clear();
        }

        this.current++;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag", "count"));
    }
}
