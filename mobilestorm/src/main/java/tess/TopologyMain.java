package tess;

import java.io.File;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		String path = new File("src/main/resources/word.txt").getAbsolutePath();

        // Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        //The spout and the bolts are connected using shuffleGroupings. This type of grouping
        //tells Storm to send messages from the source node to target nodes in randomly distributed
        //fashion.
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        // Send the same word to the same instance of the word-counter using fieldsGrouping instead of shuffleGrouping
        builder.setBolt("word-counter", new WordCounter()).fieldsGrouping("word-normalizer", new Fields("word"));

        // Configuration
        Config config = new Config();
        config.put("wordsFile", path);
         config.setDebug(true);

        // Run topology
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("my-first-topology", config, builder.createTopology());
         Thread.sleep(22000);
        localCluster.shutdown();

	}

}
