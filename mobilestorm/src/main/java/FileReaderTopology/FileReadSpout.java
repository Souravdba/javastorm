package FileReaderTopology;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class FileReadSpout extends BaseRichSpout {
	private SpoutOutputCollector _collector = null;
	private FileReader fileReader;
	BufferedReader reader=null;

	public void nextTuple() {

		try {
			String line;
			this.reader = new BufferedReader(fileReader);
			line = reader.readLine();
			while (line != null) {
				this._collector.emit(new Values(line), line);
				line = reader.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collect) {
		this._collector = collect;
		try {

			
			this.fileReader = new FileReader(conf.get("wordsFile").toString());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));

	}

}
