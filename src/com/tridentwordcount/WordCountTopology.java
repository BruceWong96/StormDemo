package com.tridentwordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import storm.trident.TridentTopology;

public class WordCountTopology {
	
	public static void main(String[] args) {
		Config conf = new Config();
		
		WordCountSpout spout = new WordCountSpout();
		
		TridentTopology tridentTopology = new TridentTopology();
		tridentTopology.newStream("spout", spout)
					//绑定切分单词组件
					.partitionAggregate(new Fields("line"), 
										new SplitAggregator(), 
										new Fields("word")
										)
					//绑定计数组件
					.partitionAggregate(new Fields("word"),
										new WordCountAggregator(),
										new Fields("word", "count")
										)
					//绑定打印组件
					.each(new Fields("word", "count"), new PrintFilter());
		
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("topology", conf, tridentTopology.build());
	}
}
