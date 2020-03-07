package com.tridentwordcount;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class SplitAggregator extends BaseAggregator<String>{

	@Override
	public String init(Object batchId, TridentCollector collector) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void aggregate(String val, TridentTuple tuple, TridentCollector collector) {
		String line = tuple.getStringByField("line");
		String [] words = line.split(" ");
		for (String word : words) {
			collector.emit(new Values(word));
		}
	}

	@Override
	public void complete(String val, TridentCollector collector) {
		// TODO Auto-generated method stub
		
	}

}
