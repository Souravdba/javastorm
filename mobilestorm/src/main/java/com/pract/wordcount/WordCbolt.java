package com.pract.wordcount;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.shade.org.apache.commons.lang.Validate;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCbolt extends BaseBasicBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2608810331084782797L;
	private Map<String, Integer> counters = new HashMap<String, Integer>();;

	public void execute(Tuple input, BasicOutputCollector collector) {
		String word = input.getString(0);
		if (!counters.containsKey(word)) {
			counters.put(word, 1);
		} else {
			Integer c = counters.get(word) + 1;
			counters.put(word, c);
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}

	
	public void cleanup() {
		 System.out.println("-- Word Counter  --");
	        for(Map.Entry<String, Integer> entry : counters.entrySet()){
	            System.out.println(entry.getKey()+": "+entry.getValue());
	        }

	}

}
