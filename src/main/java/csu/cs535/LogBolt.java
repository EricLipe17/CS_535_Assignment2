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
        return e1.getValue().compareTo(e2.getValue());
    }
}

public class LogBolt extends BaseRichBolt {
    long last_log_time;
    ConcurrentHashMap<String, Long> count_structure = new ConcurrentHashMap<>();
    FileWriter fw;
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
            this.fw = new FileWriter("/tmp/hashtag_counts.log", false);
            this.bw = new BufferedWriter(fw);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Map<String, ArrayList<Long>> structure = (Map<String, ArrayList<Long>>)tuple.getValue(0);
        Long count;
        for (Map.Entry<String, ArrayList<Long>> entry : structure.entrySet()) {
            final String hashtag = entry.getKey();
            count = this.count_structure.get(hashtag);
            if (count == null) {
                count = 0L;
            }
            count++;
            this.count_structure.put(hashtag, count);
        }
        long curr_time = System.currentTimeMillis() / 1000L;
        if (curr_time - this.last_log_time >= 10) {
            List<Map.Entry<String, Long>> entries = new LinkedList<>(this.count_structure.entrySet());
            entries.sort(new EntryComparator());

            ArrayList<String> topHashtags = new ArrayList<>(100);
            int loopVal = Math.min(entries.size(), 100);
            for (int i = 0; i < loopVal; i++) {
                topHashtags.add(entries.get(i).getKey());
            }
            String log_line = curr_time + " " + StringUtils.join(topHashtags, ",");

            try {
                this.bw.write(log_line);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            this.last_log_time = System.currentTimeMillis() / 1000L;
        }
    }

    @Override
    public void cleanup() {
        try {
            this.bw.flush();
            this.bw.close();

            this.fw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
