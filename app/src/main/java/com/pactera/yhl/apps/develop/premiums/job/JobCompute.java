package com.pactera.yhl.apps.develop.premiums.job;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity02;
import com.pactera.yhl.apps.develop.premiums.premise.mid.InsertHbase;
import com.pactera.yhl.entity.source.Lbpol;
import com.pactera.yhl.transform.TestMapTransformFunc;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


import java.util.Properties;

public class JobCompute {
    public static void Lc (StreamExecutionEnvironment env, String inputTopic, Properties prop,
                           String tableName){
        prop.setProperty("group.id","JobCompute_Lc");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, PremiumsKafkaEntity02>() {
                    @Override
                    public PremiumsKafkaEntity02 map(String s) throws Exception {
                        return JSON.parseObject(s,PremiumsKafkaEntity02.class);
                    }

                })

                .keyBy(new KeySelector<PremiumsKafkaEntity02, Tuple3<String,String,String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(PremiumsKafkaEntity02 premiumsKafkaEntity02) throws Exception {

                        return Tuple3.of("86",premiumsKafkaEntity02.branch_name,premiumsKafkaEntity02.workarea);
                    }
                }).reduce(new ReduceFunction<PremiumsKafkaEntity02>() {

                    @Override
                    public PremiumsKafkaEntity02 reduce(PremiumsKafkaEntity02 premiumsKafkaEntity02, PremiumsKafkaEntity02 t1) throws Exception {
                        return null;
                    }
                });

//                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));

    }
}
