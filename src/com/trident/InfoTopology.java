package com.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;

public class InfoTopology {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		//创建环境参数对象
		Config conf = new Config(); 
		/**
		 * 1参：发生tuple的key字段
		 * 2参：批大小
		 * 3参：发生tuple的值
		 */
		
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("name","age"), 
				2, 
				new Values("tom",12),
				new Values("rose",22),
				new Values("jerry",23),
				new Values("jack",18),
				new Values("tim",19));
		
		spout.setCycle(true);
		
		//获取Trident框架的拓扑构建者
		TridentTopology topology = new TridentTopology();
		//绑定数据源
		topology.newStream("spout", spout)
			.each(new Fields("name","age"), new NameFilter())
			.each(new Fields("name","age"), new PrintFilter());
		
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("TridentTopology", conf, topology.build());
	}
}
