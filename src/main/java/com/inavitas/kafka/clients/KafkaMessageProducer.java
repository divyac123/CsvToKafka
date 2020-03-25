package com.inavitas.kafka.clients;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class KafkaMessageProducer {

    final Producer<String, String> producer;
    private final String topicName;

  
    public KafkaMessageProducer(final Producer<String, String> producer,
                                  final String topicName) {
        this.producer = producer;
        this.topicName =topicName;
    }

    public void close() {
        this.producer.close();
    }

    public void send(final String message) {
    	System.out.println("Message is  "+ message);
        producer.send(new ProducerRecord<>(this.topicName, message));
   
    }
}