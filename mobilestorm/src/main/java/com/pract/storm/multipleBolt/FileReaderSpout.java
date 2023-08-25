package com.pract.storm.multipleBolt;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class FileReaderSpout extends BaseRichSpout {
	private static final long serialVersionUID = -5645716149114247854L;
	private SpoutOutputCollector _collector;
	FileReader flr;

	public void nextTuple() {
		BufferedReader bfd = new BufferedReader(this.flr);
		String line = null;
		try {
			while ((line = bfd.readLine()) != null) {
				this._collector.emit("Stream1", new Values(line));
				this._collector.emit("Stream2", new Values(line));

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

		this._collector = collector;
		String file = conf.get("file").toString();
		try {
			this.flr = new FileReader(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

//		declarer.declare(new Fields("line"));
		declarer.declareStream("Stream1", new Fields("line"));
		declarer.declareStream("Stream2", new Fields("line"));
	}

}
