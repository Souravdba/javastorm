package BasicTopology;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class Basicspout extends BaseRichSpout {

	private SpoutOutputCollector _collector = null;
	private String[] cdrRecords = { "1492791343|subscriber-error", "1492794343|network-error",
			"1492791343|network-error", "1492794343|network-error" };

	public void nextTuple() {
		// TODO Auto-generated method stub

		Random rn = new Random();
		String cdrnext = cdrRecords[rn.nextInt(cdrRecords.length)];
		String[] cdrde = cdrnext.split("\\|");
		System.out.println(cdrnext);
		this._collector.emit(new Values(cdrde[0], cdrde[1]));

	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

		this._collector = collector;

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("timestamp", "error-type"));
	}

}
