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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.inavitas.message.models.DataList;
import com.inavitas.message.models.FileData;

public class MessageProcessor {
	
	private ObjectMapper mapper = new ObjectMapper();
	ExecutorService executorService =  Executors.newFixedThreadPool(200);
	//Process metod will read the csv file line by line and submit the task to the executor service
    public void process(
            final BufferedReader bufferedReader) throws IOException{
        try {
        	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss"); // This is used to change the format to time in milliseconds
            bufferedReader.lines().skip(1).filter(x -> x!=null && !x.isEmpty()).forEach(line -> {
            	  try {
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
   	           	   String record = mapper.writeValueAsString(new FileData(deviceId,deviceTime,dataList));
   	           	   KafkaMessageProducer messageProducer= new KafkaMessageProducer(record);
   	           	   executorService.submit(messageProducer);  // Submitting the task to exceutor service 
   	           	 
                 }
                 catch (ParseException e) {
   				  System.out.println("Leaving the Record as there is bad data got ParseException in the Record  "+line + " Processing next-->");
                 }
                 catch (Exception e) {
   				  System.out.println("Leaving the Record as there is bad data in the  Record "+line + " Processing next-->");
                 }
            });
            shutDown();
       
        } finally {
            bufferedReader.close();
        }
    }
    // This is to shutdown the executor service after task has been completed
    public void shutDown() {
    	Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executorService.shutdown();
            try {
                executorService.awaitTermination(200, TimeUnit.MILLISECONDS);
                KafkaMessageProducer.kafkaProducer.close();
            } catch (InterruptedException e) {
               System.out.println("Exception occured while shutting down the executor service  "+e.getMessage());
            }
        }));
    }
}
