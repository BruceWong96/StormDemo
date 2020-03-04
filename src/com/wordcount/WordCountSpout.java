package com.wordcount;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordCountSpout extends BaseRichSpout{

	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;
	
	private String[] lines = new String[] {
			"hello world",
			"hello hello",
			"hello storm",
			"storm word"
	};
	
	private int index = 0;
	
	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
		
	}
	
	@Override
	public void nextTuple() {
		String line = lines[index];
		collector.emit(new Values(line));
		index++;
		if (index == lines.length) {
			index = 0;
		}
		
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	

	
	
}
