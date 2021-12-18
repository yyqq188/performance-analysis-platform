package com.pactera.yhl.apps.develop.kafka2clickhouse;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationGeneralResult;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationProductDetial;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationProductResult;
import com.pactera.yhl.apps.develop.premiums.premise.mid.InsertHbase;
import com.pactera.yhl.apps.develop.premiums.sink.InsertClickhouseOnly;
import com.pactera.yhl.entity.source.Lbpol;
import com.pactera.yhl.transform.TestMapTransformFunc;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import scala.App;

import java.util.Properties;

public class JobKafka2CK {
    public static void applicationGeneralResult(StreamExecutionEnvironment env,
                                                String topic, Properties prop,
                                String tableName){
        prop.setProperty("group.id","applicationGeneralResult");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, ApplicationGeneralResult>() {
                    @Override
                    public ApplicationGeneralResult map(String s) throws Exception {
                        return JSON.parseObject(s, ApplicationGeneralResult.class);
                    }
                })
                .addSink(new InsertClickhouseOnly<>(tableName));
    }

    public static void applicationProductResult(StreamExecutionEnvironment env,
                                                String topic, Properties prop,
                                                String tableName){
        prop.setProperty("group.id","applicationProductResult");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, ApplicationProductResult>() {
            @Override
            public ApplicationProductResult map(String s) throws Exception {
                return JSON.parseObject(s, ApplicationProductResult.class);
            }
        })
                .addSink(new InsertClickhouseOnly<>(tableName));
    }

    public static void applicationProductDetial(StreamExecutionEnvironment env,
                                                String topic,
                                                Properties prop,
                                                String tableName){
        prop.setProperty("group.id","applicationProductDetial");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, ApplicationProductDetial>() {
                    @Override
                    public ApplicationProductDetial map(String s) throws Exception {
                        return JSON.parseObject(s, ApplicationProductDetial.class);
                    }
                })
                .addSink(new InsertClickhouseOnly<>(tableName));
    }
}
