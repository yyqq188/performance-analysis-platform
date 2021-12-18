package com.pactera.yhl.apps.measure.aggregate;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.measure.entity.FactWageBase;
import com.pactera.yhl.apps.measure.entity.Measure2;
import com.pactera.yhl.apps.measure.sink.InsertClickHouseSink;
import com.pactera.yhl.apps.measure.sink.InsertHbaseSink;
import com.pactera.yhl.apps.measure.sink.InsertKafkaSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.*;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/11/11 13:55
 */
public class Calculation {

    public static void calculatePremium(StreamExecutionEnvironment env, String topic, Properties prop){
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
//        kafkaConsumer.setStartFromEarliest();

        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.print();
        SingleOutputStreamOperator<FactWageBase> operator = source.map(x -> JSON.parseObject(x, Measure2.class))
                .map(new MapFunction<Measure2, Tuple2<String, Measure2>>() {
                    @Override
                    public Tuple2<String, Measure2> map(Measure2 value) throws Exception {
                        return Tuple2.of(value.getSales_id(), value);
                    }
                })
                .keyBy(x -> x.f0)
                .map(new AssessMap());

//        operator.addSink(new InsertHbaseSink());

        operator.addSink(new InsertKafkaSink("factwagebase"));
        operator.addSink(new InsertClickHouseSink());
    }
}
