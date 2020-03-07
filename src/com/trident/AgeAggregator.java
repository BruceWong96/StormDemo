package com.trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class AgeAggregator extends BaseAggregator<Integer>{

	private int ageSum = 0;
	
	@Override
	public Integer init(Object batchId, TridentCollector collector) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void aggregate(Integer val, TridentTuple tuple, TridentCollector collector) {
		int age = tuple.getIntegerByField("age");
		ageSum = ageSum + age;
	}

	@Override
	public void complete(Integer val, TridentCollector collector) {
		collector.emit(new Values(ageSum));
	}
	
}
