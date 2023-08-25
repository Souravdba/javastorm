package com.pract.storm.mulbolt;

import java.io.File;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

	public static void main(String[] args) {

		TopologyBuilder tbd = new TopologyBuilder();
		String path = new File("C:/Users/Sourav/Documents/Storm/Data/emp.txt").getAbsolutePath();
		tbd.setSpout("MySpout", new FileReadingSpout());
		tbd.setBolt("VIPBolt", new VIPbolt()).shuffleGrouping("MySpout", "StreamVIP");
		tbd.setBolt("NormalBolt", new Normalbolt()).shuffleGrouping("MySpout", "StreamNorm");
		Config conf = new Config();
		conf.put("file",path);
		conf.setDebug(true);

		LocalCluster loc = new LocalCluster();
		loc.submitTopology("Ml", conf, tbd.createTopology());

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
