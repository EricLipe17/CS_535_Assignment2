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
    BufferedWriter bw;

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
            this.bw = new BufferedWriter(new FileWriter("/tmp/hashtag_counts.log", false));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Map<String, ArrayList<Long>> count_structure = (ConcurrentHashMap<String, ArrayList<Long>>)tuple.getValue(0);
        Long count;
        for (Map.Entry<String, ArrayList<Long>> entry : count_structure.entrySet()) {
            final String hashtag = entry.getKey();
            count = this.hashtag_counts.get(hashtag);
            if (count == null) {
                count = entry.getValue().get(0);
            }
            else {
                count += entry.getValue().get(0);
            }
            this.hashtag_counts.put(hashtag, count);
        }
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

            try {
                this.bw.write(log_line.toString());
                this.bw.newLine();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            this.last_log_time = curr_time;
        }
    }

    @Override
    public void cleanup() {
        try {
            this.bw.flush();
            this.bw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
