package csu.cs535;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class EntryComparator implements Comparator<Map.Entry<String, Long>> {
    @Override
    public int compare(Map.Entry<String, Long> e1, Map.Entry<String, Long> e2) {
        return e2.getValue().compareTo(e1.getValue());
    }
}

public class LogBolt extends BaseRichBolt {
    long last_log_time;
    ConcurrentHashMap<String, Long> hashtag_counts = new ConcurrentHashMap<>();
    FileWriter fw;

    OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.last_log_time = System.currentTimeMillis() / 1000L;
        try {
            this.fw = new FileWriter("/s/chopin/a/grad/ericlipe/hashtag_counts.log", false);
            this.fw.write(String.format("Start of log file at time %d\n", this.last_log_time));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        long count;
        final String hashtag = tuple.getString(0);
        final long latest_count = tuple.getLong(1);
        if (this.hashtag_counts.get(hashtag) == null) {
            count = latest_count;
        }
        else {

            count = latest_count + this.hashtag_counts.get(hashtag);
        }
        this.hashtag_counts.put(hashtag, count);

        long curr_time = System.currentTimeMillis() / 1000L;
        if (curr_time - this.last_log_time >= 10) {
            List<Map.Entry<String, Long>> entries = new LinkedList<>(this.hashtag_counts.entrySet());
            entries.sort(new EntryComparator());

            int loopVal = Math.min(entries.size(), 100);
            ArrayList<String> topHashtags = new ArrayList<>(loopVal);
            for (int i = 0; i < loopVal; i++) {
                topHashtags.add(entries.get(i).getKey());
            }

            StringBuilder log_line = new StringBuilder("<" + curr_time + ">");
            for (String ht : topHashtags) {
                log_line.append(String.format("<%s>", ht));
            }
            log_line.append("\n");

            try {
                this.fw.write(log_line.toString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            this.last_log_time = curr_time;
        }
    }

    @Override
    public void cleanup() {
        try {
            this.fw.flush();
            this.fw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
