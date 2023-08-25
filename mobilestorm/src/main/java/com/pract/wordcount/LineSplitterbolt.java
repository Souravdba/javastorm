package com.pract.wordcount;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class LineSplitterbolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4375778232933125500L;
	private OutputCollector oco;

	public void execute(Tuple input) {
		String line = input.getStringByField("line");
		String[] words = line.split(" ");
		for (String string : words) {
			String word = string.trim();
			if (!word.isEmpty()) {
				word = word.toLowerCase();
				oco.emit(new Values(word));
			}
		}

	}

	public void prepare(Map conf, TopologyContext context, OutputCollector collect) {
		this.oco = collect;

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
