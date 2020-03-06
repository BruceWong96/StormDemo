package com.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class WordCountTopology {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		//创建环境参数对象
		Config conf = new Config(); 
		
		//并发度设置为2，默认是1
		conf.setNumWorkers(2);
		
		WordCountSpout spout = new WordCountSpout();
		SplitBolt splitBolt = new SplitBolt();
		CountBolt countBolt = new CountBolt();
		PrintBolt printBolt = new PrintBolt();
		
		//创建拓扑构建者对象，通过这个对象可以绑定组件之间的关系 
		TopologyBuilder builder = new TopologyBuilder();
		
		//绑定数据源组件，1参：组件id，要求唯一性，2参：组件的实例对象
		builder.setSpout("wordCountSpout", spout);
		//通过传入上游的组件id，产生绑定关系
		builder.setBolt("splitBolt", splitBolt).globalGrouping("wordCountSpout");
		builder.setBolt("countBolt", countBolt).globalGrouping("splitBolt");
		builder.setBolt("printBolt", printBolt).globalGrouping("countBolt");
		
		//创建拓扑对象
		StormTopology topology = builder.createTopology();
		
		//storm的本地运行对象。 本地模式一般用于测试
//		LocalCluster cluster = new LocalCluster();
		
		//集群模式提交
		StormSubmitter cluster = new StormSubmitter();
		
		//运行拓扑，1参：拓扑id，要求唯一 2参：storm环境对象 3参：拓扑对象
		cluster.submitTopology("wordCountTopology", conf, topology);
	}
}
