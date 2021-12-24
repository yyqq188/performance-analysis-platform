package com.pactera.yhl.apps.develop.kafka2clickhouse;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationGeneralResult;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationProductDetial;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationProductResult;
import com.pactera.yhl.apps.develop.premiums.sink.InsertClickhouseOnly;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Objects;
import java.util.Properties;

public class JobKafka2CKV2 {
    public static void applicationGeneralResult(StreamExecutionEnvironment env,
                                                String topic, Properties prop,
                                String tableName){
        prop.setProperty("group.id","applicationGeneralResult");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        SingleOutputStreamOperator<String> normalStream = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                if (Objects.isNull(jsonObject.get("columnName"))) {
                    return true;
                } else {
                    return false;
                }

            }
        });

        SingleOutputStreamOperator<String> columnNameStream = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                if (!Objects.isNull(jsonObject.get("columnName"))) {
                    return true;
                } else {
                    return false;
                }

            }
        });


        normalStream.map(new MapFunction<String, ApplicationGeneralResult>() {
                    @Override
                    public ApplicationGeneralResult map(String s) throws Exception {
                        return JSON.parseObject(s, ApplicationGeneralResult.class);
                    }
                })
                .addSink(new InsertClickhouseOnly<>(tableName));



        columnNameStream.map(new AppGeneralResultHbaseMapFunc("KLMIDAPPRUN:AppGeneralResultLC"))   //
                .filter(Objects::nonNull)
        .map(new MapFunction<String, ApplicationGeneralResult>() {

                    @Override
                    public ApplicationGeneralResult map(String s) throws Exception {
                        return JSON.parseObject(s, ApplicationGeneralResult.class);
                    }
                })
                .filter(x -> x.getHesi_prem_day() < 0)
                .addSink(new InsertClickhouseOnly<>(tableName));


    }

    public static void applicationProductResult(StreamExecutionEnvironment env,
                                                String topic, Properties prop,
                                                String tableName){
        prop.setProperty("group.id","applicationProductResult");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        SingleOutputStreamOperator<String> normalStream = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                if (Objects.isNull(jsonObject.get("columnName"))) {
                    return true;
                } else {
                    return false;
                }

            }
        });

        SingleOutputStreamOperator<String> columnNameStream = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                if (!Objects.isNull(jsonObject.get("columnName"))) {
                    return true;
                } else {
                    return false;
                }

            }
        });
        normalStream.map(new MapFunction<String, ApplicationProductResult>() {
            @Override
            public ApplicationProductResult map(String s) throws Exception {
                return JSON.parseObject(s, ApplicationProductResult.class);
            }
        }).addSink(new InsertClickhouseOnly<>(tableName));


        columnNameStream.map(new AppProductResultHbaseMapFunc("KLMIDAPPRUN:AppProductResultLC"))   //
                .filter(Objects::nonNull)
                .map(new MapFunction<String, ApplicationProductResult>() {
            @Override
            public ApplicationProductResult map(String s) throws Exception {
                return JSON.parseObject(s, ApplicationProductResult.class);
            }
        }).addSink(new InsertClickhouseOnly<>(tableName));
    }

    public static void applicationProductDetial(StreamExecutionEnvironment env,
                                                String topic,
                                                Properties prop,
                                                String tableName){
        prop.setProperty("group.id","applicationProductDetial");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        SingleOutputStreamOperator<String> normalStream = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                if (Objects.isNull(jsonObject.get("columnName"))) {
                    return true;
                } else {
                    return false;
                }

            }
        });

        SingleOutputStreamOperator<String> columnNameStream = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                if (!Objects.isNull(jsonObject.get("columnName"))) {
                    return true;
                } else {
                    return false;
                }

            }
        });
        normalStream.map(new MapFunction<String, ApplicationProductDetial>() {
                    @Override
                    public ApplicationProductDetial map(String s) throws Exception {
                        return JSON.parseObject(s, ApplicationProductDetial.class);
                    }
                }).addSink(new InsertClickhouseOnly<>(tableName));

        columnNameStream.map(new AppProductDetailHbaseMapFunc("KLMIDAPPRUN:AppProductDetailLC"))   //
                .filter(Objects::nonNull)
                .map(new MapFunction<String, ApplicationProductDetial>() {
            @Override
            public ApplicationProductDetial map(String s) throws Exception {
                return JSON.parseObject(s, ApplicationProductDetial.class);
            }
        }).addSink(new InsertClickhouseOnly<>(tableName));
    }
}
