package com.inavitas.main;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.inavitas.kafka.clients.CSVReader;
import com.inavitas.kafka.clients.KafkaMessageProducer;
import com.inavitas.message.models.FileData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.util.List;
import java.util.Properties;


public class MessageProcessor {

    private static final String SERVER = "localhost:9092";
    private static final String TOPIC_NAME = "electric-data";
    private static ObjectMapper mapper = new ObjectMapper();
    private static KafkaMessageProducer kafkaProducer;
    static {
    	 final Properties props = new Properties();
         props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
         props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                 "org.apache.kafka.common.serialization.StringSerializer");
         props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                 "org.apache.kafka.common.serialization.StringSerializer");
         final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
         kafkaProducer =
                 new KafkaMessageProducer(
                         producer,
                         TOPIC_NAME);
    }
 
    public static void main(String[] args) throws IOException, ParseException {

    	MessageProcessor msgproducer = new MessageProcessor();
        final File csvFile = msgproducer.getFileFromResources("data.csv");
     
        final List<FileData> dataList =  new CSVReader()
                                .loadCsvContentToList(new BufferedReader(new FileReader(csvFile)));
        if(dataList.isEmpty()) {
            System.out.println("No Data Found in File");
            return;
        }
       
        dataList.forEach(data -> {
            try {
            	kafkaProducer.send(mapper.writeValueAsString(data));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            } finally {
            	kafkaProducer.close();
            }
        });
    }
    
    
    private File getFileFromResources(String fileName) {

        ClassLoader classLoader = getClass().getClassLoader();

        URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("file is not found!");
        } else {
            return new File(resource.getFile());
        }

    }
}
