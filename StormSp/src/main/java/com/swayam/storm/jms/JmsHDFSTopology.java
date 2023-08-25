package com.swayam.storm.jms;
import javax.jms.Session;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.MoveFileAction;
import org.apache.storm.jms.JmsProvider;
import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.jms.spout.JmsSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class JmsHDFSTopology {
	public static final String JMS_QUEUE_SPOUT = "JMS_QUEUE_SPOUT";
    public static final String INTERMEDIATE_BOLT = "INTERMEDIATE_BOLT";
    public static final String FINAL_BOLT = "FINAL_BOLT";
    public static final String JMS_TOPIC_BOLT = "JMS_TOPIC_BOLT";
    public static final String JMS_TOPIC_SPOUT = "JMS_TOPIC_SPOUT";
    public static final String ANOTHER_BOLT = "ANOTHER_BOLT";

    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {

        // JMS Queue Provider
    	
    	
        JmsProvider jmsQueueProvider = new SpringJmsProvider(
                "jms-activemq.xml", "jmsConnectionFactory",
                "notificationQueue");
        // JMS Producer
        JmsTupleProducer producer = new JsonTupleProducer();

        // JMS Queue Spout
        JmsSpout queueSpout = new JmsSpout();
        queueSpout.setJmsProvider(jmsQueueProvider);
        queueSpout.setJmsTupleProducer(producer);
        queueSpout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        queueSpout.setDistributed(true); // allow multiple instances
        
        TopologyBuilder builder = new TopologyBuilder();

        // spout with 5 parallel instances
        builder.setSpout(JMS_QUEUE_SPOUT, queueSpout, 6);

        // intermediate bolt, subscribes to jms spout, anchors on tuples, and auto-acks
        builder.setBolt(INTERMEDIATE_BOLT,
                new GenericBolt("INTERMEDIATE_BOLT", true, true, new Fields("CallDetail")), 8).shuffleGrouping(
                JMS_QUEUE_SPOUT);
        
//        builder.setBolt(ANOTHER_BOLT,
//        		new procJmsbolt(),8).shuffleGrouping(
//        				INTERMEDIATE_BOLT);
//        
        
        SyncPolicy syncPolicy = new CountSyncPolicy(10);
        FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, TimedRotationPolicy.TimeUnit.MINUTES);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/tmp/storm-data")
                .withExtension(".txt");
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");
        
        HdfsBolt hdbolt = new HdfsBolt()
                .withConfigKey("hdfs.config")
                .withFsUrl("hdfs://192.168.99.100:9000")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy)
                .addRotationAction(new MoveFileAction().toDestination("/tmp/dest2/"));
        
        builder.setBolt(FINAL_BOLT, hdbolt, 3).shuffleGrouping(
        		INTERMEDIATE_BOLT);

        Config conf = new Config();

        if (args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf,
                    builder.createTopology());
        } else {

            conf.setDebug(true);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("storm-jms", conf, builder.createTopology());
            Utils.sleep(80000);
            cluster.killTopology("storm-jms");
            cluster.shutdown();
        }
    }

}
