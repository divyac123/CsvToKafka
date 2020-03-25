package com.inavitas.message.models;


import java.util.Date;
import java.util.List;
import java.util.Map;

public class FileData {

	private String deviceId;
	private Date dateTime;
	private List<Map<String,String>> dataList;
	

    public FileData(String deviceId,Date dateTime,List<Map<String,String>> dataList) {
        this.deviceId = deviceId;
        this.dateTime = dateTime;
        this.dataList = dataList;
    }

    public String getDeviceId() {
		return deviceId;
	}

	public Date getDateTime() {
		return dateTime;
	}

	public List<Map<String,String>> getDataList() {
		return dataList;
	}
	
}
