package io.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;


public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello World I am a Kafka Consumer");

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

        //create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for data
        while(true) {

            log.info("polling");

            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis((1000)));

            for(ConsumerRecord<String, String> record : records){
                log.info("Key: " + record.key() + ", Value: " + record.value() );
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset() );
            }
        }


    }
}
