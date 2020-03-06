package com.ack.wordcount;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitBolt extends BaseRichBolt{

	private OutputCollector collector;

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
	
	@Override
	public void execute(Tuple input) {
		try {
			
	
		String line = input.getStringByField("line");
		String [] words = line.split(" ");
		
		for (String word : words) {
			collector.emit(input, new Values(word));
		}
		//返回ACK给上游
		collector.ack(input);
		} catch (Exception e) {
			//返回失败给上游
			collector.fail(input);
		}
		
	}

}
