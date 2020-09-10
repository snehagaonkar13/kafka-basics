package com.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreadsSeekAndAssign {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreadsSeekAndAssign.class.getName());
    private static final String BOOTSTRAP_SERVERS ="127.0.0.1:9092";
    //private static final String GROUP_ID ="my-sixth-group";
    private static final String TOPIC = "first_topic";

    public static void main(String[] args) {
        logger.info( "Consumer ::");

    new ConsumerDemoWithThreadsSeekAndAssign().run();


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
            //kafkaConsumer.subscribe(Collections.singleton(TOPIC));

            //Assign  seek and replay data or fetch a specific message

            //Assign
            TopicPartition topicPartition = new TopicPartition(TOPIC,0);
            kafkaConsumer.assign(Arrays.asList(topicPartition));
            Long offsetToReadFrom = 3L;
            //Seek - Go to specific Offsets
            kafkaConsumer.seek(topicPartition,offsetToReadFrom);



        }
        @Override
        public void run() {
            int numberOfMessagesToReadFrom = 5;
            boolean keepOnReading =true;
            int messagesReadSoFar = 0;
            try {
                //Poll for new data
                while (keepOnReading) {
                    ConsumerRecords<String, String> consumerRecords =
                            kafkaConsumer.poll(Duration.ofMillis(100));// New in Kafka 2.0.0
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        messagesReadSoFar +=1;
                        logger.info("Key ::" + consumerRecord.key() + "\n" + "Value ::" + consumerRecord.value() + "\n" +
                                "Partition::" + consumerRecord.partition() + "\n" + "Offset ::" + consumerRecord.offset());
                    if(messagesReadSoFar>numberOfMessagesToReadFrom){
                        keepOnReading =false;// exit while
                        break;//exit for loop
                    }
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
            //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,GROUP_ID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            return properties;
        }
    }
}
