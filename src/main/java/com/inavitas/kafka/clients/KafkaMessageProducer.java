package com.inavitas.kafka.clients;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
//This class is a thread -- run method will be called for each device id.
public final class KafkaMessageProducer implements Runnable {

    private String message;
    public static KafkaProducer<String,String> kafkaProducer;
    private static final String SERVER = "localhost:9092";
    private static final String TOPIC_NAME = "electric-data";
   
    
    static {
    	 final Properties props = new Properties();
         props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
         //key and value serializer are used to convert key and value objects of data to bytes for the transmission of data over the network
         props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                 "org.apache.kafka.common.serialization.StringSerializer");  
         props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                 "org.apache.kafka.common.serialization.StringSerializer");
         kafkaProducer = new KafkaProducer<>(props);
      
    }

    public static KafkaProducer<String, String> getKafkaProducer() {
		return kafkaProducer;
	}

	public KafkaMessageProducer(String message) {
        this.message = message;
    }
    // This mthod we need to overwrite if we are implementing Runnable interface. 
	//For each task this method will be run by executor service
	@Override
	public void run() {
		System.out.println("Message is  "+ this.message);
		kafkaProducer.send(new ProducerRecord<String, String>(this.TOPIC_NAME, this.message));	 
	}
	
	
    
}