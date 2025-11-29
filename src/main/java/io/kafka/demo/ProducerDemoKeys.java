package io.kafka.demo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello World From ProducerDemoWithCallback");

        //create producer properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("acks", "all");



        //set producer preoperty
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for(int j=0; j<2; j++){
            for(int i = 0; i < 10; i++) {
                String topic = "topic1";
                String key = "id_" + i;
                String value = "hello world" + i;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                //send data async operation
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            log.info("Key " + key + " | Partition: " + metadata.partition() + " | Value: " + value);
                        }
                        else {
                            log.error("Error in producing record ", exception);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //flush and close the producer
        producer.flush(); // tell the producer to send all data and block until done --sync operation
        producer.close();
    }
}
