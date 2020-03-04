package com.wordcount;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CountBolt extends BaseRichBolt{


	private static final long serialVersionUID = 1L;

	private OutputCollector collector;
	
	private Map<String, Integer> map = new HashMap<>();
	
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","count"));
	}
	
	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		
		if (map.containsKey(word)) {
			map.put(word, map.get(word)+1);
		}else {
			map.put(word, 1);
		}
		//Tuple是K V对的集合，可以有一个，也可以有多个
		//Key 和 Value 的数量即对应关系保持 一致
		collector.emit(new Values(word, map.get(word)));
	}

	

}
