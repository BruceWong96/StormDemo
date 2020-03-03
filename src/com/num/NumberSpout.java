package com.num;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 开发一个Spout（数据源）组件
 * 本例中，此数据源的作用是产生一个个的随机数字，向下游的的bolt发送
 * 
 * @author wenjie
 *
 */
public class NumberSpout extends BaseRichSpout{

	//数据源发射器，通过这个组件将tuple发送给下游
	private SpoutOutputCollector collector;
	
	/**
	 * 此方法是初始化方法，所以此方法用于组件的初始化
	 */
	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	/**
	 * 此方法用于声明向下游发送tuple的key字段名字
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//--通过Fields来声明tuple的key字段，可以有多个
		declarer.declare(new Fields("number"));
		
	}
	
	/**
	 * 此方法用于产生数据，并向下游发射tuple
	 * 此方法会被调用多次，注意：不要将此方法变为阻塞方法
	 */
	@Override
	public void nextTuple() {
		//产生一个100以内的随机数
		int number = new Random().nextInt(100);
		
		//把数字封装到tuple里，然后通过发射器发射出去
		collector.emit(new Values(number));
		
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	

	

}
