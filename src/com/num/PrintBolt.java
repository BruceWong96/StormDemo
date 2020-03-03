package com.num;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class PrintBolt extends BaseRichBolt{

	//创建Bolt组件的发射器，用于向下游发射bolt
	private OutputCollector collector;
	
	/**
	 * 此方法是初始化bolt组件方法
	 */
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		
	}
	
	/**
	 * 此方法是bolt组件用于接收上游发来的tuple
	 * 如果有下游组件，也会在此方法中发射tuple
	 */
	@Override
	public void execute(Tuple input) {
		//通过tuple的key取值
		int number = input.getIntegerByField("number");
		
		System.out.println("数字"+number);
	}


	

}
