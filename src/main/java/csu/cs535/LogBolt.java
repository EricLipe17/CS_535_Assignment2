package csu.cs535;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

class Pair {
    String first;
    long second;

    public Pair(String first, long second) {
        this.first = first;
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public long getSecond() {
        return second;
    }
}

class EntryComparator implements Comparator<Map.Entry<String, Long>> {
    @Override
    public int compare(Map.Entry<String, Long> e1, Map.Entry<String, Long> e2) {
        return e1.getValue().compareTo(e2.getValue());
    }
}

public class LogBolt extends BaseBasicBolt {
    long last_log_time;
    Map<String, Long> count_structure = new HashMap<>();
    FileWriter fw = new FileWriter("hashtag_counts.log", true);
    BufferedWriter bw = new BufferedWriter(fw);

    public LogBolt() throws IOException {
        this.last_log_time = System.currentTimeMillis() / 1000L;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
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
            List<Map.Entry<String, Long>> entries = new LinkedList<Map.Entry<String, Long>>(this.count_structure.entrySet());
            entries.sort(new EntryComparator());

            ArrayList<String> topHashtags = new ArrayList<>(100);
            for (int i = 0; i < 100; i++) {
                topHashtags.add(entries.get(i).getKey());
            }
            String log_line = StringUtils.join(topHashtags, ",");

            try {
                this.bw.write(log_line);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            this.last_log_time = System.currentTimeMillis() / 1000L;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }
}
