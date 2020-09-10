package com.kafka.basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerDemo {
    private static final String BOOTSTRAP_SERVERS ="127.0.0.1:9092";
    public static void main(String[] args) {
        System.out.println("Kafka Producer ..");
        //Create Producer Properties
        Properties properties = getProperties();
        //Create Producer

        KafkaProducer<String ,String> kafkaProducer =
                new KafkaProducer<String, String>(properties);
        //Create Producer Record
        ProducerRecord<String,String> producerRecord =
                new ProducerRecord<String, String>("first_topic","Check if working");
        //Send Data
        kafkaProducer.send(producerRecord);
        kafkaProducer.flush();
        kafkaProducer.close();
        System.out.println("Message Received");
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        return properties;
    }


}
