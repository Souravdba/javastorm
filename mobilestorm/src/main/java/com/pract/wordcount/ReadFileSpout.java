package com.pract.wordcount;

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

public class ReadFileSpout extends BaseRichSpout {

	private static final long serialVersionUID = 6420084856644586581L;
	FileReader fld;
	private SpoutOutputCollector _collector;

	public void nextTuple() {

		BufferedReader bfd = new BufferedReader(fld);
		String line = null;
		try {
			while ((line = bfd.readLine()) != null) {
				this._collector.emit(new Values(line), line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

		this._collector = collector;
		String filename = conf.get("File").toString();
		try {
			this.fld = new FileReader(filename);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));

	}

	private void clean() {
		// TODO Auto-generated method stub
		try {
			this.fld.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void ack(Object msgId) {
		System.out.println("Successd" + msgId);
	}

	@Override
	public void fail(Object msgId) {
		System.out.println("Fail" + msgId);
	}

}
