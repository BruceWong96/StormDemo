package com.trident;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class PrintFilter extends BaseFilter{

	@Override
	public boolean isKeep(TridentTuple tuple) {
		String name = tuple.getStringByField("name");
		int age = tuple.getIntegerByField("age");
		System.out.println(name + ":" + age);
		return false;
	}
	
}
