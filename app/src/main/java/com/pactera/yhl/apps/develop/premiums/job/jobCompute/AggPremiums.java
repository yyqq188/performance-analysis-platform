package com.pactera.yhl.apps.develop.premiums.job.jobCompute;

import com.pactera.yhl.entity.source.Lcpol;
import com.pactera.yhl.apps.develop.premiums.sink.PremiumsKuduSinkV2;
import com.pactera.yhl.transform.TestMapTransformFunc;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class AggPremiums {
    public static void premiums(StreamExecutionEnvironment env, String topic, Properties prop) {
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop
        );

        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        SingleOutputStreamOperator<Tuple4<String, String, String, String>> sum = env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Lcpol)
                .map(x -> (Lcpol) x)
                .keyBy(new KeySelector<Lcpol, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(Lcpol lcpol) throws Exception {
                        return Tuple3.of(lcpol.getGrppolno(), lcpol.getPolno(), lcpol.getInsuredno());
                    }
                }).reduce(new ReduceFunction<Lcpol>() {
                    @Override
                    public Lcpol reduce(Lcpol lcpol, Lcpol t1) throws Exception {
                        Lcpol newLc = new Lcpol();
                        Double pre = Double.valueOf(lcpol.getPrem());
                        Double suf = Double.valueOf(t1.getPrem());
                        newLc.setPrem(String.valueOf(pre + suf));
                        return newLc;
                    }
                }).map(new MapFunction<Lcpol, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> map(Lcpol lcpol) throws Exception {
                        return Tuple4.of(lcpol.getGrppolno(), lcpol.getPolno(), lcpol.getInsuredno(), lcpol.getPrem());
                    }
                });
        sum.addSink(new PremiumsKuduSinkV2());

    }
}