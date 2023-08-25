package com.storm.shuffle;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class Numberbolt extends BaseRichBolt {
	private static final long serialVersionUID = 7820767706790045537L;
	private OutputCollector _collector;
	private PrintWriter _prwr;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

		this._collector = collector;
		String fileName = "output" + "-" + "-" + context.getThisComponentId() + context.getThisTaskId() + ".txt";
		try {
			this._prwr = new PrintWriter(conf.get("dirToWrite").toString() + fileName, "UTF-8");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

	}

	public void execute(Tuple tuple) {
		String str = tuple.getStringByField("integer");
		String buck = tuple.getStringByField("bucket");
		this._prwr.write(str +"-"+ buck +"\n");
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void cleanup() {
		this._prwr.close();
	}

}
