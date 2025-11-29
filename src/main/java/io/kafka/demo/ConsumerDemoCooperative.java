package io.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello World I am a Kafka Consumer who gracefully shuts down");

        String groupId = "my-java-application";
        String topic = "topic1";

        //create consumer properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");



        //set consumer preoperty
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());

        props.setProperty("group.id", groupId);

        props.setProperty("auto.offset.reset", "earliest");

        props.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

//        props.setProperty("group.instance.id", "...."); //strategy for static assignments

        //create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // for shutdown get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        //adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()");
                consumer.wakeup();

                // join the amin thread to allow the execution of the code in main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });



        // try catch as consumer can throw exception on wakeup as wakeup exception at some point for time
        try{
            //subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            //poll for data
            while(true) {

                ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis((1000)));

                for(ConsumerRecord<String, String> record : records){
                    log.info("Key: " + record.key() + ", Value: " + record.value() );
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset() );
                }
            }
        }catch (WakeupException we){
            log.info("Wakeup triggered in the consumer, exiting");
        }catch (Exception e){
            log.info("Unhandled exception in the consumer ", e);
        }finally{
            consumer.close();
            log.info("Consumer is gracefully shut down");
        }
    }
}
