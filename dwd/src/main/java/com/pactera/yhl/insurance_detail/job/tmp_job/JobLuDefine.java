package com.pactera.yhl.insurance_detail.job.tmp_job;

import com.pactera.yhl.entity.*;
import com.pactera.yhl.insurance_detail.map.MapFuncTableAnychatcont;
import com.pactera.yhl.insurance_detail.sink.TmpLbcontSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;


import java.util.Properties;

public class JobLuDefine {

    public static void lupol_from_lcpol(StreamExecutionEnvironment env, Properties properties, String topic,String kafkaGroupId) throws Exception {
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof Lcpol)
                .map(x -> x.toString())
                .addSink(new FlinkKafkaProducer<String>("10.5.2.133:6667,10.5.2.134:6667,10.5.2.144:6667,10.5.2.145:6667",
                        "tmptopic", new SimpleStringSchema()));
        env.execute("TmpLcpolSink");
    }

    public static void lupol_from_lbpol(StreamExecutionEnvironment env, Properties properties, String topic,String kafkaGroupId) throws Exception {
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof Lbpol)
                .map(x -> x.toString())
                .addSink(new FlinkKafkaProducer<String>("10.5.2.133:6667,10.5.2.134:6667,10.5.2.144:6667,10.5.2.145:6667",
                        "tmptopic", new SimpleStringSchema()));
        env.execute("TmpLbpolSink");
    }
    public static void lucont_from_lccont(StreamExecutionEnvironment env, Properties properties, String topic,String kafkaGroupId) throws Exception {
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof Lccont);
//                .addSink(new TmpLbcontSink());
        env.execute("TmpLccontSink");
    }
    public static void lucont_from_lbcont(StreamExecutionEnvironment env, Properties properties, String topic,String kafkaGroupId) throws Exception {
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof Lbcont);
//                .addSink(new TmpLbcontSink());
        env.execute("TmpLbcontSink");
    }
}
