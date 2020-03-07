package com.tridentwordcount;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordCountSpout extends BaseRichSpout{

	private SpoutOutputCollector collector;
	
	String[] lines = new String[]{
			"hello world",
			"hello",
			"nihao storm",
			"hello spout",
			"hello bolt"
	};
	
	int index = 0;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		String line = lines[index];
		index++;
		collector.emit(new Values(line));
		if (index == lines.length) {
			index = 0;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

}
