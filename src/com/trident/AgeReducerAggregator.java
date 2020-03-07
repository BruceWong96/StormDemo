package com.trident;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

public class AgeReducerAggregator implements ReducerAggregator<Integer>{

	/**
	 * 此方法是组件的初始化方法，会产生curr，并传给reducer方法
	 */
	@Override
	public Integer init() {
		return 0;
	}

	@Override
	public Integer reduce(Integer curr, TridentTuple tuple) {
		// TODO Auto-generated method stub
		int age = tuple.getIntegerByField("age");
		int ageSum = curr + age;
		return ageSum;
	}

}
