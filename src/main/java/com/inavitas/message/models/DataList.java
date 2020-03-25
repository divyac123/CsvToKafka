package com.inavitas.message.models;

import java.util.HashMap;
import java.util.Map;

public class DataList {

	private String dataName;
	private String dataValue;
	
	public DataList() {

	}
	
	public DataList(String dataName,String dataValue) {
		this.dataName=dataName;
		this.dataValue= dataValue;
		
	}

	public Map<String,String> getMap(){
		Map<String,String>  map = new HashMap<>();
		map.put("dataName", this.dataName);
		map.put("dataValue",this.dataValue);
		return map;
	}
}
