package com.pract.storm.mulbolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class Normalbolt extends BaseBasicBolt {

	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		String line = input.getString(0);
		System.out.println("[Normal]" + " " + line);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("lineB"));
	}

}
