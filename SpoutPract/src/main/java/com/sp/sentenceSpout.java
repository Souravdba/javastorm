package com.sp;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class sentenceSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	FileReader fh;
	BufferedReader bfd;
	private boolean completed = false;

	Random rand;
	public void open(Map conf, TopologyContext Context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		rand=new Random();
		try
		{
		fh=new FileReader(conf.get("inputFile").toString());
		}
			catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}	
		
		this.collector=collector;
	}
	public void nextTuple() {
		// TODO Auto-generated method stub
		  
//		if (completed) {
//			try {
//			  Thread.sleep(1500);
//			} catch (InterruptedException e) {
//
//			}
//		}
		
		String str;
		BufferedReader reader = new BufferedReader(fh);
		try {
			while ((str = reader.readLine()) != null) {
				this.collector.emit(new Values(str), str);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error reading typle", e);
		} finally {
			completed = true;
		}
	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("Sentence"));
	}

	@Override
	public void close() {
		try {
			fh.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
