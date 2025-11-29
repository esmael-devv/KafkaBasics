package io.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello World From ProducerDemo");

        //create producer properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("acks", "all");



        //set producer preoperty
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("topic1", "Hello World from ProducerDemo");


        //send data async operation
        producer.send(producerRecord);

        //flush and close the producer
        producer.flush(); // tell the producer to send all data and block until done --sync operation
        producer.close();
    }
}
