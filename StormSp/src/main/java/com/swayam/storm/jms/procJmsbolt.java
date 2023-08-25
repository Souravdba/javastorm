package com.swayam.storm.jms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class procJmsbolt extends BaseRichBolt{

	private OutputCollector collector;
	private Fields declaredFields;
	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		List<String> Alldata = Arrays.asList(tuple.getString(0).split(",")).subList(1, 2);
//				Collections.singletonList((Object)tuple.getString(0).split(","));
		List<Object> objectList = (List)Alldata;
		collector.emit(objectList);
		
	}

	@Override
	public void prepare(Map cont, TopologyContext tuple, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("IMSI","IMEI","DeviceName","Longitude","Latitude","SourceOSCategory","SourceOSCategory","DeviceType",
				"MarketingVendor","CallDuration","TimeStamp"));
		
	
	}


}
