package com.pract.storm.mulbolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class VIPbolt extends BaseBasicBolt {

	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getString(0);
		System.out.println("[VIP]" + " " + line);

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("lineA"));

	}

}
