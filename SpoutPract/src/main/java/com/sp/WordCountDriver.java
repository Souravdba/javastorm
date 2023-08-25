package com.sp;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountDriver {
public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException {
	TopologyBuilder tb=new TopologyBuilder();
	tb.setSpout("sentence", new sentenceSpout(),2);
	tb.setBolt("wordsplit", new wordSplitBolt(),2).shuffleGrouping("sentence");
	tb.setBolt("wordcount", new wordCntBolt(),2).fieldsGrouping("wordsplit", new Fields("word"));
	tb.setBolt("report-bolt", new reportbolt(),2).globalGrouping("wordcount");
	Config conf=new Config();
	conf.setDebug(true);
//	conf.put("dirToWrite", "C:\\Users\\Sourav\\Documents\\Storm\\files");
	
	if(args != null && args.length >0 )
	{
		conf.put("inputFile",args[0]);
		conf.put("dirToWrite", args[1]);
		conf.setNumWorkers(3);
		StormSubmitter.submitTopologyWithProgressBar("Wordcount", conf, tb.createTopology());
	}
	else{
		conf.put("inputFile", "C:\\Users\\Sourav\\Documents\\Storm\\demo.txt");
		conf.setMaxTaskParallelism(6);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("My First Topology", conf, tb.createTopology());
		Thread.sleep(15000);
		cluster.shutdown();	
	}
	
}
}
