package BasicTopology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class BasicTopology {
	public static void main(String[] args) {

		TopologyBuilder tb=new TopologyBuilder();
		tb.setSpout("Basic Spout", new Basicspout());
		tb.setBolt("Basic Bolt", new BasicBolt());
		Config con=new Config();
		con.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		try {
			cluster.submitTopology("My-First-Topology", con, tb.createTopology());
			Thread.sleep(12000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		

		finally {
			cluster.shutdown();
		}
		
	}
}
