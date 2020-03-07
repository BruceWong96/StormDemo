package com.trident;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class AgeCombinerAggregator implements CombinerAggregator<Integer>{

	private static final long serialVersionUID = 2L;

	/**
	 * 此方法是组件初始化方法，此方法会返回一个初始值 val1，
	 * val1会传给combine方法
	 */
	@Override
	public Integer zero() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	/**
	 * 此方法会接收上游发来的tuple
	 * 此方法会返回val2值，传给combine方法
	 */
	@Override
	public Integer init(TridentTuple tuple) {
		int age = tuple.getIntegerByField("age");
		return age;
	}

	
	@Override
	public Integer combine(Integer val1, Integer val2) {
		int ageSum = val1 + val2;
		return ageSum;
	}
	
	
	
}
