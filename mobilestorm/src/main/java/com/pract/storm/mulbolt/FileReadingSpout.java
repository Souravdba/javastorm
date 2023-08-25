package com.pract.storm.mulbolt;

import java.io.BufferedReader;
import java.io.File;
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

public class FileReadingSpout extends BaseRichSpout  {

	private SpoutOutputCollector _collector;
	FileReader flr;

	public void nextTuple() {

		BufferedReader bfd = new BufferedReader(this.flr);
		String line = null;
		try {
			while ((line = bfd.readLine()) != null) {
				String[] words = line.split(",");
				if (words[1].equals("VIP")) {
					this._collector.emit("StreamVIP", new Values(line));
				}

				else {
					this._collector.emit("StreamNorm", new Values(line));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

		this._collector = collector;
		String filename = new File(conf.get("file").toString()).getAbsolutePath();
		try {
			this.flr = new FileReader(filename);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declareStream("StreamVIP", new Fields("lineA"));
		declarer.declareStream("StreamNorm", new Fields("lineB"));

	}

}
