package com.kafka.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class KafkaProducerWithKeysDemo {
    private static final String BOOTSTRAP_SERVERS ="127.0.0.1:9092";
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerWithKeysDemo.class);
    private static final String KAFKA_TOPIC = "second-topic";
    public static void main(String[] args) {
        String datatoBeSent = null;
        String key = null;
        logger.info("Kafka Producer ..");
        //Create Producer Properties
        Properties properties = getProperties();
        //Create Producer

        KafkaProducer<String ,String> kafkaProducer =
                new KafkaProducer<String, String>(properties);

        for (int i=0; i<10; i++) {
            datatoBeSent = "Check if working" + Integer.toString(i);
            key = "id_" +Integer.toString(i);
            //Create Producer Record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<String, String>(KAFKA_TOPIC, datatoBeSent);
            //Send Data
            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null)
                        logger.info("Record processed successfully" + "\n" +
                                "Topic :" + recordMetadata.topic() + "\n" +
                                "Partition :" + recordMetadata.partition() + "\n" +
                                "Offset :" + recordMetadata.offset() + "\n" +
                                "Timestamp :" + recordMetadata.timestamp());
                    else
                        logger.error("Error In processing", e);
                }
            });
        }
        kafkaProducer.flush();
        kafkaProducer.close();
        logger.info("Message Received");
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        return properties;
    }


}
