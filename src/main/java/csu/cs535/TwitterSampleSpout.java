package csu.cs535;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Map;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

@SuppressWarnings("serial")
public class TwitterSampleSpout extends BaseRichSpout {
   SpoutOutputCollector collector;
		
   // ADD FILE PATH TO hastags.txt BELOW
   // ADD FILE PATH TO hastags.txt BELOW
   // ADD FILE PATH TO hastags.txt BELOW
   String fileName = "/s/chopin/a/grad/ericlipe/hashtags.txt";
   // ADD FILE PATH TO hastags.txt ABOVE
   // ADD FILE PATH TO hastags.txt ABOVE
   // ADD FILE PATH TO hastags.txt ABOVE

   private LineNumberReader in;
		
   public TwitterSampleSpout() {
   }
		
   @Override
   public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      this.collector = collector;
      try {
         in = new LineNumberReader(new FileReader(fileName));
      } catch (FileNotFoundException e){
         System.err.print("######## FileNotFoundException: " + e);
      }
   }
			
   @Override
   public void nextTuple() {
      if (in == null) return;
      String line = null;
         
      try {
         line = in.readLine();
      } catch (IOException e) {
         System.err.print("######## IOException: " + e);
      }
      
      if (line != null) {
         // System.out.println("SPOUT " + line);
         collector.emit(new Values(line));
      }
   }
			
   @Override
   public void close() {
      if (in == null) return;
      try {
         in.close();
      } catch (IOException e) {
         System.err.print("######## IOException: " + e);
      }
   }
			
   @Override
   public void ack(Object msgId) {
   }
			
   @Override
   public void fail(Object msgId) {
   }
			
   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("hashtag"));
   }
}