package com.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
    private static final String BOOTSTRAP_SERVERS ="127.0.0.1:9092";
    private static final String GROUP_ID ="my-fourth-group";
    private static final String TOPIC = "first_topic";
    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        return properties;
    }
    public static void main(String[] args) {
        logger.info( "Consumer ::");
        //Create Consumer Properties
        Properties properties = getProperties();

        //Create Consumer
        KafkaConsumer<String,String> kafkaConsumer =
                        new KafkaConsumer<String, String>(properties);

        //Subscribe to topic
        kafkaConsumer.subscribe(Collections.singleton(TOPIC));

        //Poll for new data
        while (true){
           ConsumerRecords<String,String> consumerRecords =
                   kafkaConsumer.poll(Duration.ofMillis(100));// New in Kafka 2.0.0
            for (ConsumerRecord<String,String> consumerRecord : consumerRecords){
                logger.info("Key ::" + consumerRecord.key() +"\n" + "Value ::" + consumerRecord.value() + "\n"+
                "Partition::"+consumerRecord.partition() +"\n" + "Offset ::" +consumerRecord.offset());
            }
        }
    }
}
