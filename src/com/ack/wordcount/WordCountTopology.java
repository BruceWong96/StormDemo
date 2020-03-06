package com.ack.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopology {
	
	public static void main(String[] args) {
		Config conf = new Config(); 
		WordCountSpout wordCountSpout = new WordCountSpout();
		SplitBolt splitBolt = new SplitBolt();
		WordCountBolt wordCountBolt = new WordCountBolt();
		PrintBolt printBolt = new PrintBolt();
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("wordCountSpout", wordCountSpout);
		builder.setBolt("splitBolt", splitBolt).shuffleGrouping("wordCountSpout");
		builder.setBolt("wordCountBolt", wordCountBolt).fieldsGrouping("splitBolt",new Fields("word"));
		builder.setBolt("printBolt", printBolt).globalGrouping("wordCountBolt");
		
		StormTopology stormTopology = builder.createTopology();
		
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("AckWordCountTopology", conf, stormTopology);
		
	}
}
