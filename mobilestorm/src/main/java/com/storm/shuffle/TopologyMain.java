package com.storm.shuffle;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologyMain {

	public static void main(String[] args) {
		TopologyBuilder tbd=new TopologyBuilder();
		tbd.setSpout("1stspot", new PrintingNumber());
		tbd.setBolt("1stbolt", new Numberbolt(),2).fieldsGrouping("1stspot", new Fields("bucket"));
		Config conf=new Config();
		conf.setDebug(true);
		conf.put("dirToWrite", "./files/");
		LocalCluster lcu=new LocalCluster();
		lcu.submitTopology("1sttopo", conf, tbd.createTopology());
		try {
			Thread.sleep(15000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			lcu.shutdown();
		}
	}
	
}
