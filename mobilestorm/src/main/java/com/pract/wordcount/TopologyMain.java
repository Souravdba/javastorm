package com.pract.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologyMain {

	public static void main(String[] args) {
		
		TopologyBuilder tbd=new TopologyBuilder();
		tbd.setSpout("FileReaderSpout", new ReadFileSpout());
		tbd.setBolt("LineSplitter", new LineSplitterbolt()).shuffleGrouping("FileReaderSpout");
		tbd.setBolt("WordCount", new WordCbolt()).fieldsGrouping("LineSplitter", new Fields("word"));
		Config conf=new Config();
		conf.put("File", "C:/Users/Sourav/Documents/Storm/demo.txt");
		conf.setDebug(true);
		LocalCluster loc=new LocalCluster();
		loc.submitTopology("wordcount", conf, tbd.createTopology());
		try {
			Thread.sleep(25000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			loc.shutdown();
		}
	}

}
