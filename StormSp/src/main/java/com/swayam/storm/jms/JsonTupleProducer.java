package com.swayam.storm.jms;

import java.util.Collections;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import redis.clients.jedis.Jedis;

@SuppressWarnings("serial")
public class JsonTupleProducer implements JmsTupleProducer {
	
	public Values toTuple(Message msg) throws JMSException {
		if (msg instanceof ActiveMQBytesMessage) {
			// String json = ((TextMessage) msg).getText();
			ActiveMQBytesMessage textMessage = (ActiveMQBytesMessage) msg;
			String json = new String(textMessage.getContent().data);
			//Below Added to filter
			String[] arr=json.split(",");
			Jedis jedis = new Jedis("192.168.99.103",6379);
			String Desc=jedis.hget("calldesc", arr[1]);
			if(Desc != null)
			{
				arr[1]=Desc;
			}
			else
			{
				arr[1]="UN";
			}
			//
			String Finals=String.join(",", arr);
//			return new Values(arr[0],arr[1],arr[2],arr[3],arr[4],arr[5],arr[6],arr[7],arr[8],arr[9],arr[10]);
		    return new Values(Finals);
		} else {
			return null;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declare(new Fields("IMSI","CallEndReas","IMEI","DeviceName","Longitude","Latitude","SourceOSCategory","DeviceType",
//				"MarketingVendor","CallDuration","TimeStamp"));
		
		declarer.declare(new Fields("CallDetail"));
	}

}
