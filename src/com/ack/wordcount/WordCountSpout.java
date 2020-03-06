package com.ack.wordcount;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.sound.sampled.Line;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountSpout extends BaseRichSpout{

	private SpoutOutputCollector collector;
	
	String[] lines = new String[]{"hello world",
						"hello storm",
						"haha haha",
						"nihao storm"};
	//索引
	int index = 0;
	
	//
	private Map<UUID, Values> map;
	
	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		this.collector = collector;
		
		map = new HashMap<UUID, Values>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

	@Override
	public void nextTuple() {
		String line = lines[index];
		UUID msgId = UUID.randomUUID();
		
		//存储UUID对应的tuple id
		map.put(msgId, new Values(line));
		//Storm at least once(至少一次语义);
		//数据源在发射tuple时要跟上一个全局唯一的ID
		collector.emit(new Values(line), msgId);
		
		index++;
		//循环发送数据
		if (index == lines.length) {
			index = 0;
		}
		
	}
	
	
	//如果下游收到ACK，则将map中存储的tuple id移除，否则可能会造成内存溢出
	@Override
	public void ack(Object msgId) {
		map.remove(msgId);
	}
	
	
	//当下游的组件反馈fail时，会进入此方法
	//我们需要根据失败的tuple 的 id，重新发射一次
	@Override
	public void fail(Object msgId) {
		collector.emit(map.get(msgId), msgId);
	}



	
	
	
	
}
