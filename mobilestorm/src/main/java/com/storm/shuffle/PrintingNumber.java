package com.storm.shuffle;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class PrintingNumber extends BaseRichSpout {

	private static final long serialVersionUID = 403763499080731998L;
	private SpoutOutputCollector _collect=null;
	private Integer inn = 0;

	public void nextTuple() {

		while (this.inn <= 100) {
			Integer bucket = this.inn / 10;
			this.inn++;
			this._collect.emit(new Values(this.inn.toString(),bucket.toString()));
		}

	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this._collect = collector;

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("integer","bucket"));
	}

}
