package com.pactera.yhl.hivekafka3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaClient {
    public static KafkaProducer getProducer(){
        Properties kafkaProps = new Properties();
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("bootstrap.servers","10.5.2.133:6667,10.5.2.134:6667,10.5.2.144:6667,10.5.2.145:6667");

        return new KafkaProducer<String, String>(kafkaProps);
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer producer = KafkaClient.getProducer();
        producer.send(new ProducerRecord("lupol","aaa")).get();
    }


}
