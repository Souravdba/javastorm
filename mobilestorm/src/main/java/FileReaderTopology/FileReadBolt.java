package FileReaderTopology;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FileReadBolt extends  BaseBasicBolt{

	public void execute(Tuple input , BasicOutputCollector collector) {
		String line = input.getStringByField("line");
//	    collector.emit(new Values(line));
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
//		declarer.declare(new Fields("line"));
	}

	

}
