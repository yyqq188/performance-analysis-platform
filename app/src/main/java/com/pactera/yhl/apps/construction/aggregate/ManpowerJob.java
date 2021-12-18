package com.pactera.yhl.apps.construction.aggregate;

import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.construction.entity.EffectiveManpower;
import com.pactera.yhl.apps.construction.statejob.*;
import com.pactera.yhl.apps.construction.util.DateUDF;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author: TSY
 * @create: 2021/11/16 0016 下午 16:40
 * @description:  人力计算层
 */
public class ManpowerJob {
    public static void construction(StreamExecutionEnvironment env, String topic, Properties kafkaprops) {
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), kafkaprops
        );

        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        //kafkaConsumer.setStartFromEarliest();

        //将json转换成实体类对象
        SingleOutputStreamOperator<EffectiveManpower> source = env.addSource(kafkaConsumer)
                .map(new MapFunction<String, EffectiveManpower>() {
                    @Override
                    public EffectiveManpower map(String value) throws Exception {
                        System.out.println("value:::" + value);
                        return JSONObject.parseObject(value, EffectiveManpower.class);
                    }
                });

        //==================日期交分公司====================//
        /**日期缴活动人力 (当天出单signdate)*/
        source.filter(x -> {
            return "lcpol".equals(x.getMark()) && x.getSigndate().compareTo(DateUDF.getCurrentDate()) == 0;
        }).map(new MapFunction<EffectiveManpower, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(EffectiveManpower value) throws Exception {
                return Tuple3.of(value.getKey_id(), value.getSales_id(), value.getPrem());
            }
        }).keyBy(x -> x.f0)
                .map(new ActivitySumStateMap()).print("分-日期缴活动人力：");

        /**日入职出单人力（当天入职probation_date，当天出单signdate)*/
       source.filter(x -> {
            return "lcpol".equals(x.getMark()) && x.getProbation_date().compareTo(DateUDF.getCurrentDate()) == 0 && x.getSigndate().compareTo(DateUDF.getCurrentDate()) == 0;
        }).map(new MapFunction<EffectiveManpower, Tuple3<String, String, String>>() {
           @Override
           public Tuple3<String, String, String> map(EffectiveManpower value) throws Exception {
               return Tuple3.of(value.getKey_id(), value.getSales_id(), value.getPrem());
           }
       }).keyBy(x -> x.f0).map(new IssueSumStateMap()).print("分-日入职出单人力：");

        /**日合格人力  当天出单signdate)*/
        source.filter(x -> {
            return "lcpol".equals(x.getMark()) && x.getSigndate().compareTo(DateUDF.getCurrentDate()) == 0;
        }).map(new MapFunction<EffectiveManpower, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(EffectiveManpower value) throws Exception {
                return Tuple3.of(value.getKey_id(), value.getSales_id(), value.getPrem());
            }
        }).keyBy(x -> x.f0).map(new QualifiedSumStateMap()).print("分-日合格人力：");


        //==================日期交总公司====================//
        /**日期缴活动人力*/
        source.filter(x -> {
            return "lcpol".equals(x.getMark()) && x.getSigndate().compareTo(DateUDF.getCurrentDate()) == 0;
        }).map(new MapFunction<EffectiveManpower, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(EffectiveManpower value) throws Exception {
                return Tuple3.of(value.getKey_id().substring(0, 10) + "#86", value.getSales_id(), value.getPrem());
            }
        }).keyBy(x -> x.f0).map(new ActivitySumStateMap()).print("总-日期缴活动人力：");

        /**日入职出单人力（*/
        source.filter(x -> {
            return "lcpol".equals(x.getMark()) && x.getProbation_date().compareTo(DateUDF.getCurrentDate()) == 0 && x.getSigndate().compareTo(DateUDF.getCurrentDate()) == 0;
        }).map(new MapFunction<EffectiveManpower, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(EffectiveManpower value) throws Exception {
                return Tuple3.of(value.getKey_id().substring(0, 10) + "#86", value.getSales_id(), value.getPrem());
            }
        }).keyBy(x -> x.f0).map(new IssueSumStateMap()).print("总-日入职出单人力：");

        //日合格人力
        source.filter(x -> {
            return "lcpol".equals(x.getMark()) && x.getSigndate().compareTo(DateUDF.getCurrentDate()) == 0;
        }).map(new MapFunction<EffectiveManpower, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(EffectiveManpower value) throws Exception {
                return Tuple3.of(value.getKey_id().substring(0, 10) + "#86", value.getSales_id(), value.getPrem());
            }
        }).keyBy(x -> x.f0).map(new QualifiedSumStateMap()).print("总-日合格人力：");

/*
        //==================月期交分公司====================//
        //月期交活动人力(当天出单signdate)
        source.filter(x -> {
            return x.getSigndate().compareTo(firstDate) >= 0 && x.getSigndate().compareTo(lastDate) <= 0;
        }).map(x -> new Tuple3<>(x.getKey_id(), x.getSales_id(), x.getPrem()))
                .map(new MonthActivitySumStateMap());

        //月入职出单人力（当月入职,当月出单)
        source.filter(x -> {
            return x.getProbation_date().compareTo(firstDate) >= 0 && x.getProbation_date().compareTo(lastDate) <= 0 && x.getSigndate().compareTo(firstDate) >= 0 && x.getSigndate().compareTo(lastDate) <= 0;
        }).map(x -> new Tuple3<>(x.getKey_id(), x.getSales_id(), x.getPrem()))
                .map(new MonthIssueSumStateMap());

        //月合格人力(当天出单signdate)
        source.filter(x -> {
            return x.getSigndate().compareTo(firstDate) >= 0 && x.getSigndate().compareTo(lastDate) <= 0;
        }).map(x -> new Tuple3<>(x.getKey_id(), x.getSales_id(), x.getPrem()))
                .map(new MonthQualifiedSumStateMap());


        //==================月期交总公司====================//
        //月期交活动人力
        source.filter(x -> {
            return x.getSigndate().compareTo(firstDate) >= 0 && x.getSigndate().compareTo(lastDate) <= 0;
        }).map(x -> new Tuple3<>(x.getKey_id().substring(0, 10) + "#86", x.getSales_id(), x.getPrem()))
                .map(new MonthActivitySumStateMap());

        //月入职出单人力
        source.filter(x -> {
            return x.getProbation_date().compareTo(firstDate) >= 0 && x.getProbation_date().compareTo(lastDate) <= 0 && x.getSigndate().compareTo(firstDate) >= 0 && x.getSigndate().compareTo(lastDate) <= 0;
        }).map(x -> new Tuple3<>(x.getKey_id().substring(0, 10) + "#86", x.getSales_id(), x.getPrem()))
                .map(new MonthIssueSumStateMap());

        //月合格人力
        source.filter(x -> {
            return x.getSigndate().compareTo(firstDate) >= 0 && x.getSigndate().compareTo(lastDate) <= 0;
        }).map(x -> new Tuple3<>(x.getKey_id().substring(0, 10) + "#86", x.getSales_id(), x.getPrem()))
                .map(new MonthQualifiedSumStateMap());
*/
    }
}