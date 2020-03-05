package com.numstream;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class LessThanBolt extends BaseRichBolt{

	
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		
	}
	
	@Override
	public void execute(Tuple input) {
		int number = input.getIntegerByField("number");
		System.out.println("LessThanBolt:"+number);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
}
