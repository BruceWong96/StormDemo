package com.trident;

import java.util.Iterator;

import backtype.storm.tuple.Fields;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class PrintFilter extends BaseFilter{

	@Override
	public boolean isKeep(TridentTuple tuple) {
//		String name = tuple.getStringByField("name");
//		int age = tuple.getIntegerByField("age");
//		System.out.println(name + ":" + age);
		
		//取出当前tuple中所有的 key+value
		Fields fields = tuple.getFields();
		
		Iterator<String> iterator = fields.iterator();
		
		while (iterator.hasNext()) {
			String key = iterator.next();
			Object value = tuple.getValueByField(key);
			System.out.println(key + ":" + value );
		}
		
		return false;
	}
	
}
