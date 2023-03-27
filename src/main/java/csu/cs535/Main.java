package csu.cs535;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("FakeTwitterSpout", new TwitterSampleSpout());
        builder.setBolt("BucketBolt", new BucketBolt(100), 1).shuffleGrouping("FakeTwitterSpout");
        builder.setBolt("CountBolt", new SequentialCountBolt(), 1).shuffleGrouping("BucketBolt");
        builder.setBolt("LogBolt", new LogBolt(), 1).shuffleGrouping("CountBolt");

        Config conf = new Config();
        conf.setDebug(true);
        String topoName = "Sequential Count Topology";
        conf.setNumWorkers(1);
        try {
            StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
            throw new RuntimeException(e);
        }
//        long curr_time = System.currentTimeMillis() / 1000L;
//        long current = 1L;
//        ArrayList<Long> counts;
//        ConcurrentHashMap<String, ArrayList<Long>> count_structure = new ConcurrentHashMap<>();
//        String[] bucket = new String[]{"#CubaPorLaPaz", "#ballsqueeze", "#onlyfans", "#nsfwtwt", "#OylarTazminata", "#AWP23", "#Galatasaray", "#mypics", "#Splatoon", "#Splatoon3", "#NintendoSwitch", "#BuenosDias", "#poupettekenza", "#oriele", "#Shibarium", "#RYSFISK", "#RYSLANE", "#IMPORTANTE:", "#NowPlaying", "#RetroHitsCanada", "#mufc", "#Oscar", "#oscars95", "#RamCharan", "#RamCharanBossingOscars", "#OylarTazminata", "#Bills", "#london", "#jaihind", "#jaibhim", "#mizuruiweek2023", "#prsk_FA", "#gfvip", "#incorvassi", "#Eagles", "#HavadanZehirleniyoruz", "#bbb23", "#Bihar", "#viral", "#Trending", "#KeHuyQuan", "#BrendanFraser", "#dickrate", "#dm", "#nudes", "#cockrate", "#nsfw", "#bigdick", "#bwc", "#sext", "#porn", "#horny", "#dick", "#cum", "#rates", "#thickcock", "#thick", "#dickpic", "#bigcock", "#cocktribute", "#trade", "#pintoawards", "#cock", "#nsfwtwt", "#porn", "#fitness", "#youngcock", "#sext", "#nsfwt", "#dick", "#nudes", "#bbc", "#massage_in_downtown", "#massage_in_five", "#OylarTazminata", "#WWERaw", "#esenyurt", "#esenyurt", "#SULLYOON", "#NMIXX", "#Young_Dumb_Stupid", "#kerjaonline", "#WWERaw", "#esenyurt"};
//        for (int i = 0; i < bucket.length; i++) {
//            String hashtag = bucket[i];
//            counts = count_structure.get(hashtag);
//            if (counts == null) {
//                counts = new ArrayList<>(2);
//                counts.add(0L);
//                counts.add(current - 1L);
//            }
//            long count = counts.get(0);
//            count++;
//            counts.set(0, count);
//            count_structure.put(hashtag, counts);
//        }
//
//        // Prune the data structure
//        for (Map.Entry<String, ArrayList<Long>> entry : count_structure.entrySet()) {
//            final String hashtag = entry.getKey();
//            ArrayList<Long> freq_delta = entry.getValue();
//            final long sum = freq_delta.get(0) + freq_delta.get(1);
//
//            if (sum <= current) {
//                count_structure.remove(hashtag);
//            }
//        }
//
//        // Log bolt
//        Long count;
//        ConcurrentHashMap<String, Long> count_structure2 = new ConcurrentHashMap<>();
//        for (Map.Entry<String, ArrayList<Long>> entry : count_structure.entrySet()) {
//            final String hashtag = entry.getKey();
//            count = count_structure2.get(hashtag);
//            if (count == null) {
//                count = entry.getValue().get(0);
//            }
//            else {
//                count += entry.getValue().get(0);
//            }
//            count_structure2.put(hashtag, count);
//        }
//        long new_time = System.currentTimeMillis() / 1000L;
//        long time = new_time - curr_time;
//        if (time >= 10) {
//            List<Map.Entry<String, Long>> entries = new LinkedList<>(count_structure2.entrySet());
//            entries.sort(new EntryComparator());
//
//            int loopVal = Math.min(entries.size(), 100);
//            ArrayList<String> topHashtags = new ArrayList<>(loopVal);
//            for (int i = 0; i < loopVal; i++) {
//                topHashtags.add(entries.get(i).getKey());
//            }
//
//
//            StringBuilder log_line = new StringBuilder("<" + curr_time + ">");
//            for (String ht : topHashtags) {
//                log_line.append(String.format("<%s>", ht));
//            }
////            String log_line = curr_time + " " + StringUtils.join(topHashtags, ",");
//            System.out.println(log_line);
//        }
    }
}
