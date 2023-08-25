package com.pract.storm.multipleBolt;

import java.io.File;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {
	public static void main(String[] args) {
		String file = new File("C:/Users/Sourav/Documents/Storm/Data/emp.txt").getAbsolutePath();
		TopologyBuilder tbd = new TopologyBuilder();
		tbd.setSpout("Spout1", new FileReaderSpout());
		tbd.setBolt("VIPBolt", new VIPBolt()).shuffleGrouping("Spout1","Stream1");
		tbd.setBolt("NormBolt", new NormBolt()).shuffleGrouping("Spout1","Stream2");
		Config conf = new Config();
		conf.put("file", file);
		conf.setDebug(true);

		LocalCluster loc = new LocalCluster();
		loc.submitTopology("Topo1", conf, tbd.createTopology());
		try {
			Thread.sleep(22000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			loc.shutdown();
		}

	}
}
