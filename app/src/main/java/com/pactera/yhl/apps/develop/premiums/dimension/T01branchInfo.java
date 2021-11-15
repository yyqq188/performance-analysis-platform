package com.pactera.yhl.apps.develop.premiums.dimension;

import com.pactera.yhl.apps.develop.premiums.entity.DimensionT01BranchId;
import com.pactera.yhl.entity.source.Lcpol;
import com.pactera.yhl.entity.source.T01branchinfo;
import com.pactera.yhl.transform.TestMapTransformFunc;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class T01branchInfo {
//    public static void demo(){
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//        DataStreamSource<Action> actions = env.addSource(new KafkaConsumer<>());
//        DataStreamSource<Patter> patteran = env.addSource(new KafkaConsumer<>());
//        KeyedStream<OUT, ?> actionByUser = actions.keyBy((KeySelector<OUT, ? extends Object>) a -> a.id);
//        MapStateDescriptor patternDescribe = new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Patter.class));
//        BroadcastStream<Patter> broadcast = patteran.broadcast(patternDescribe);
//        actionByUser.connect(broadcast).process(new PatternEvaluator());
//
//    }
    public static void branchInfo(StreamExecutionEnvironment env, String topic, Properties prop){
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop
        );
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        SingleOutputStreamOperator<Tuple4<String, String, String, String>> source = env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof T01branchinfo)
                .map(x -> {
                    T01branchinfo branchinfo = (T01branchinfo) x;
                    return Tuple4.of(branchinfo.getBranch_id(),
                            branchinfo.getBranch_id_parent(),
                            branchinfo.getBranch_id_full(),
                            branchinfo.getBranch_name());
                });
//        source.broadcast(DimensionT01BranchId)
    }
}
