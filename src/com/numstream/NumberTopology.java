package com.numstream;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class NumberTopology {
	public static void main(String[] args) {
		//创建环境参数对象
		Config conf = new Config(); 
		NumberSpout spout = new NumberSpout();
		NumberBolt numberBolt = new NumberBolt();
		LessThanBolt lessThanBolt = new LessThanBolt();
		MoreThanBolt moreThanBolt = new MoreThanBolt();
		
		//创建拓扑构建者对象，通过这个对象可以绑定组件之间的关系 
		TopologyBuilder builder = new TopologyBuilder();
		
		//绑定数据源组件，1参：组件id，要求唯一性，2参：组件的实例对象
		builder.setSpout("numberSpout", spout);
		//通过传入上游的组件id，产生绑定关系
		builder.setBolt("numberBolt", numberBolt).shuffleGrouping("numberSpout");
		
		
		builder.setBolt("lessThanBolt", lessThanBolt).globalGrouping("numberBolt","lessThan");
		builder.setBolt("moreThanBolt", moreThanBolt).globalGrouping("numberBolt","moreThan");
		
		
		//创建拓扑对象
		StormTopology topology = builder.createTopology();
		
		//storm的本地运行对象。 本地模式一般用于测试
		LocalCluster cluster = new LocalCluster();
		
		//运行拓扑，1参：拓扑id，要求唯一 2参：storm环境对象 3参：拓扑对象
		cluster.submitTopology("numberTopo", conf, topology);
	}
}
