package com.trident;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class NameFilter extends BaseFilter{

	@Override
	public boolean isKeep(TridentTuple tuple) {
		String name = tuple.getStringByField("name");
		if (name.equals("tom")) {
			//如果姓名为tom，则选择过滤，不发送此tuple到下游
			return false;
		}else {
			return true;
		}
	}
	
}
