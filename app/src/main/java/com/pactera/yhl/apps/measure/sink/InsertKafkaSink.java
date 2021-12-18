package com.pactera.yhl.apps.measure.sink;

import com.pactera.yhl.apps.measure.entity.FactWageBase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class InsertKafkaSink extends RichSinkFunction<FactWageBase> {
    public InsertKafkaSink(String topic){
        topicName = topic;
    }
    public static String topicName = "";
    public static KafkaProducer producer = null;
    public static int i = 1;
    @Override
    public void open(Configuration parameters) throws Exception {
        Properties kafkaProps = new Properties();
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("bootstrap.servers","10.5.2.133:6667,10.5.2.134:6667,10.5.2.144:6667,10.5.2.145:6667");

        producer = new KafkaProducer<>(kafkaProps);
    }

    @Override
    public void invoke(FactWageBase value, Context context) throws Exception {
        producer.send(new ProducerRecord<>(topicName,value));
        System.out.println(i++);
    }
}
