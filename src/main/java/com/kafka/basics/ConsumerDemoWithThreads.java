package com.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());
    private static final String BOOTSTRAP_SERVERS ="127.0.0.1:9092";
    private static final String GROUP_ID ="my-second-group";
    private static final String TOPIC = "first_topic";

    public static void main(String[] args) {
        logger.info( "Consumer ::");

    new ConsumerDemoWithThreads().run();


    }
    public void run() {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Runnable runnable = new ConsumerThreads(countDownLatch);
        Thread kafkaThread = new
                Thread(runnable);
        kafkaThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught Shutdown hook");
            ((ConsumerThreads) runnable).shutDown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                logger.error("Application Interrupted::");
            }
            finally {
                logger.info("Application is Closing");
            }
        }));


        //Wait till application finishes its task
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
           logger.error("Application Interrupted::");
        }
        finally {
            logger.info("Application is Closing");
        }
    }

    public class ConsumerThreads implements  Runnable{
        /*
         * Latch will help in shutting down the application correctly
         * */
        private CountDownLatch countDownLatch;
        private KafkaConsumer<String,String> kafkaConsumer;
        //Create Consumer Properties
         private Properties properties = getProperties();

        public ConsumerThreads (CountDownLatch countDownLatch){
            this.countDownLatch = countDownLatch;

            //Create Consumer
            kafkaConsumer =
                    new KafkaConsumer<String, String>(properties);

            //Subscribe to topic
            kafkaConsumer.subscribe(Collections.singleton(TOPIC));
        }
        @Override
        public void run() {
            try {
                //Poll for new data
                while (true) {
                    ConsumerRecords<String, String> consumerRecords =
                            kafkaConsumer.poll(Duration.ofMillis(100));// New in Kafka 2.0.0
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        logger.info("Key ::" + consumerRecord.key() + "\n" + "Value ::" + consumerRecord.value() + "\n" +
                                "Partition::" + consumerRecord.partition() + "\n" + "Offset ::" + consumerRecord.offset());
                    }
                }
            }
            catch (WakeupException wakeupException){
                logger.info("Received shutdown signal::");
            }
            finally {
                kafkaConsumer.close();
                /*This tells the our main code that we are done with the consumer
                * */
                countDownLatch.countDown();
            }
        }

        public void shutDown(){
            /*
            * wakeup() Method is a special method that interrupts Consumer.poll
            * It throws Wakeup Exception
            * */
            kafkaConsumer.wakeup();

        }

        private  Properties getProperties() {
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,GROUP_ID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            return properties;
        }
    }
}
