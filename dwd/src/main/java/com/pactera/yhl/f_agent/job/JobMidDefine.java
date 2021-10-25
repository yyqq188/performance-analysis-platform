package com.pactera.yhl.f_agent.job;

import com.pactera.yhl.entity.T01branchinfo;
import com.pactera.yhl.entity.T01teaminfo;
import com.pactera.yhl.entity.T02salesinfo_k;
import com.pactera.yhl.f_agent.map.MapFuncTableAnychatcont;
import com.pactera.yhl.f_agent.sink.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author: TSY
 * @create: 2021/10/20 0020 上午 11:22
 * @description:
 */
public class JobMidDefine {

    public static void A_Branch_Id_ParentSink(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId) throws Exception {
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof T01branchinfo)
                .map(x -> (T01branchinfo) x)
                .addSink(new A_Branch_Id_ParentSink());
        env.execute("A_Branch_Id_ParentSink");
    }

    public static void A_Branch_idSink(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId) throws Exception {
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof T01branchinfo)
                .map(x -> (T01branchinfo) x)
                .addSink(new A_Branch_idSink());
        env.execute("A_Branch_idSink");
    }


    public static void OXTO_Branch_IdSink(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId) throws Exception {
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof T02salesinfo_k)
                .map(x -> (T02salesinfo_k) x)
                .addSink(new OXTO_Branch_IdSink());
        env.execute("OXTO_Branch_IdSink");
    }


    public static void OXTT_Team_Id_ParentSink(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId) throws Exception {
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof T01teaminfo)
                .map(x -> (T01teaminfo) x)
                .addSink(new OXTT_Team_Id_ParentSink());
        env.execute("OXTT_Team_Id_ParentSink");
    }


    public static void OXTT_Team_IdSink(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId) throws Exception {
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof T01teaminfo)
                .map(x -> (T01teaminfo) x)
                .addSink(new OXTT_Team_IdSink());
        env.execute("OXTT_Team_IdSink");
    }

    public static void OXTO_Team_IdSink(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId) throws Exception {
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof T02salesinfo_k)
                .map(x -> (T02salesinfo_k) x)
                .addSink(new OXTO_Team_IdSink());
        env.execute("OXTO_Team_IdSink");
    }

}
