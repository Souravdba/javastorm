package com.pract.storm.multipleBolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class NormBolt extends BaseBasicBolt {

	public void execute(Tuple input, BasicOutputCollector collector) {

		String line = input.getString(0);
		System.out.println("From Norm" + line);

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));

	}

}
