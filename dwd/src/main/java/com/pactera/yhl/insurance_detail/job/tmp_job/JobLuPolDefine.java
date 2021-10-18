package com.pactera.yhl.insurance_detail.job.tmp_job;

import com.pactera.yhl.entity.*;
import com.pactera.yhl.insurance_detail.map.MapFuncTableAnychatcont;

import com.pactera.yhl.insurance_detail.process.filter.TmpLbpolFilter;
import com.pactera.yhl.insurance_detail.process.filter.TmpLcpolFilter;
import com.pactera.yhl.insurance_detail.process.map.TmpLbpolMap;
import com.pactera.yhl.insurance_detail.process.map.TmpLcpolMap;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;


import java.util.Map;
import java.util.Properties;

public class JobLuPolDefine {
    public static void lupol_from_lcpol(StreamExecutionEnvironment env, Properties properties,
                                        String topic,String kafkaGroupId) throws Exception {
        Map<String, String> configMap = env.getConfig().getGlobalJobParameters().toMap();
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof Lcpol)
                .map(x -> (Lcpol) x)
                .filter(new TmpLcpolFilter())
                .map(new TmpLcpolMap())
                .map(x -> x.toString())
                .addSink(new FlinkKafkaProducer<String>(configMap.get("kafka_bootstrap_servers"),
                        configMap.get("kafka_topic_lupol"), new SimpleStringSchema()));
        env.execute("TmpLcpolSink");
    }

    public static void lupol_from_lbpol(StreamExecutionEnvironment env, Properties properties, String topic,String kafkaGroupId) throws Exception {

        Map<String, String> configMap = env.getConfig().getGlobalJobParameters().toMap();
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof Lbpol)
                .map(x -> (Lbpol) x)
                .filter(new TmpLbpolFilter())
                .map(new TmpLbpolMap())
                .map(x -> x.toString())
                .addSink(new FlinkKafkaProducer<String>(configMap.get("kafka_bootstrap_servers"),
                        configMap.get("kafka_topic_lupol"), new SimpleStringSchema()));
        env.execute("TmpLbpolSink");
    }

}
