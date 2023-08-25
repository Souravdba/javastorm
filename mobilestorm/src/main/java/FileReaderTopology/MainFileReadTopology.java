package FileReaderTopology;

import java.io.File;
import java.io.IOException;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class MainFileReadTopology {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String path = new File("src/main/resources/word.txt").getAbsolutePath();
		TopologyBuilder tbd = new TopologyBuilder();
		tbd.setSpout("File-Spout", new FileReadSpout());
		tbd.setBolt("File-bolt", new FileReadBolt());
		Config conf=new Config();
		conf.setDebug(false);
        conf.put("wordsFile", path);
        conf.setDebug(true);
		
		LocalCluster loc=new LocalCluster();
		try {
			
			loc.submitTopology("File-Read", conf, tbd.createTopology());
			Thread.sleep(22000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			loc.shutdown();
		}
		
	}

}
