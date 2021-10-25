package com.pactera.yhl.f_agent.job;
import com.pactera.yhl.entity.T01branchinfo;
import com.pactera.yhl.entity.T01teaminfo;
import com.pactera.yhl.entity.T02salesinfo_k;
import com.pactera.yhl.f_agent.join.*;
import com.pactera.yhl.f_agent.map.MapFuncTableAnychatcont;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author: TSY
 * @create: 2021/10/20 0020 下午 17:30
 * @description:
 */
public class JobJoinDefine {
    public static void jobFAJoinATeam(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId){
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> envsource = env.addSource(kafkaConsumer);
        SingleOutputStreamOperator<T01teaminfo> source = envsource.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof T01teaminfo)
                .map(x -> (T01teaminfo) x);

        AsyncDataStream.unorderedWait(source,new FAJoinATeam(),10, TimeUnit.SECONDS,10);
    }
    public static void jobFAJoinBranch(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId){
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> envsource = env.addSource(kafkaConsumer);
        SingleOutputStreamOperator<T01branchinfo> source = envsource.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof T01branchinfo)
                .map(x -> (T01branchinfo) x);

        AsyncDataStream.unorderedWait(source,new FAJoinBranch(),10, TimeUnit.SECONDS,10);
    }
    public static void jobFAJoinFatherBranch(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId){
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> envsource = env.addSource(kafkaConsumer);
        SingleOutputStreamOperator<T01branchinfo> source = envsource.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof T01branchinfo)
                .map(x -> (T01branchinfo) x);

        AsyncDataStream.unorderedWait(source,new FAJoinFatherBranch(),10, TimeUnit.SECONDS,10);
    }

    public static void jobFAJoinGrandBranch(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId){
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> envsource = env.addSource(kafkaConsumer);
        SingleOutputStreamOperator<T01branchinfo> source = envsource.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof T01branchinfo)
                .map(x -> (T01branchinfo) x);

        AsyncDataStream.unorderedWait(source,new FAJoinGrandBranch(),10, TimeUnit.SECONDS,10);
    }
    public static void jobFAJoinPersonnel(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId){
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> envsource = env.addSource(kafkaConsumer);
        SingleOutputStreamOperator<T02salesinfo_k> source = envsource.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof T02salesinfo_k)
                .map(x -> (T02salesinfo_k) x);

        AsyncDataStream.unorderedWait(source,new FAJoinPersonnel(),10, TimeUnit.SECONDS,10);
    }
    public static void jobFAJoinRTeam(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId){
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> envsource = env.addSource(kafkaConsumer);
        SingleOutputStreamOperator<T01teaminfo> source = envsource.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof T01teaminfo)
                .map(x -> (T01teaminfo) x);

        AsyncDataStream.unorderedWait(source,new FAJoinRTeam(),10, TimeUnit.SECONDS,10);
    }
    public static void jobFAJoinSonBranch(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId){
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> envsource = env.addSource(kafkaConsumer);
        SingleOutputStreamOperator<T01branchinfo> source = envsource.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof T01branchinfo)
                .map(x -> (T01branchinfo) x);

        AsyncDataStream.unorderedWait(source,new FAJoinSonBranch(),10, TimeUnit.SECONDS,10);
    }
    public static void jobFAJoinTeam(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId){
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> envsource = env.addSource(kafkaConsumer);
        SingleOutputStreamOperator<T01teaminfo> source = envsource.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof T01teaminfo)
                .map(x -> (T01teaminfo) x);

        AsyncDataStream.unorderedWait(source,new FAJoinTeam(),10, TimeUnit.SECONDS,10);
    }
}
