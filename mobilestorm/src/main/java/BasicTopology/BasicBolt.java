package BasicTopology;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class BasicBolt extends BaseRichBolt {

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		long timestamp=Long.parseLong(input.getStringByField("timestamp"));
		String errorType = input.getStringByField("error-type");
		SimpleDateFormat date = new SimpleDateFormat("dd/MM/yyyy");
		System.out.println("Date= "+date.format(new Date(timestamp)));
		
	}

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

}
