package io.kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
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
        props.setProperty("batch.size", "400");
//        props.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for(int j=0; j<10; j++){
            for(int i = 0; i < 30; i++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("topic1", "Hello World from ProducerDemoWithCallback" + i);


                //send data async operation
               producer.send(producerRecord, new Callback() {
                   @Override
                   public void onCompletion(RecordMetadata metadata, Exception exception) {
                       if (exception == null) {
                           log.info("Received  new metadata \n"+
                                   "Topic " + metadata.topic() +"\n" +
                                   "Partition " + metadata.partition() + "\n" +
                                   "Offset " + metadata.offset() + "\n" +
                                   "Timestamp " + metadata.timestamp());
                       }
                       else {
                           log.error("Error in producing record ", exception);
                       }
                   }
               });
            }
            try {
                Thread.sleep(500);
            }catch(InterruptedException e){
                e.printStackTrace();
            }
        }

        //create a producer record
        for(int i = 0; i < 30; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("topic1", "Hello World from ProducerDemoWithCallback" + i);


            //send data async operation
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Error while producing", exception);
                } else {
                    log.info("Sent to topic={}, partition={}, offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        }


        //flush and close the producer
        producer.flush(); // tell the producer to send all data and block until done --sync operation
        producer.close();
    }
}
