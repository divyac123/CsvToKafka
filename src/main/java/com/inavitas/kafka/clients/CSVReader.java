package com.inavitas.kafka.clients;

import java.io.BufferedReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.inavitas.message.models.DataList;
import com.inavitas.message.models.FileData;

public class CSVReader {
   
    public List<FileData> loadCsvContentToList(
            final BufferedReader bufferedReader) throws IOException, ParseException {
        try {
        	List<FileData> mainlist = new ArrayList<>();
        	bufferedReader.readLine();
        	String line;
        	SimpleDateFormat sdf = new SimpleDateFormat("d/M/yyyy h:mm");
            while(!(line = bufferedReader.readLine()).isBlank())
            {
               List<Map<String,String>> dataList = new ArrayList<>();
           	   List<String> list = Arrays.asList(line.split(","));
           	   String deviceId = list.get(0);
           	   Date deviceTime = sdf.parse(list.get(1));
           	   dataList.add(new DataList("CurrentA",list.get(2)).getMap());
           	   dataList.add(new DataList("CurrentB",list.get(3)).getMap());
           	   dataList.add(new DataList("CurrentC",list.get(4)).getMap());
           	   dataList.add(new DataList("ActivePower",list.get(5)).getMap());
           	   dataList.add(new DataList("AppearentPower",list.get(6)).getMap());
           	   dataList.add(new DataList("ReactivePower",list.get(7)).getMap());
           	   dataList.add(new DataList("VoltageA",list.get(8)).getMap());
           	   dataList.add(new DataList("VoltageB",list.get(9)).getMap());
           	   dataList.add(new DataList("VoltageC",list.get(10)).getMap());
           	   mainlist.add(new FileData(deviceId,deviceTime,dataList));
            }
            return mainlist;
       
        } finally {
            bufferedReader.close();
        }
    }
}
