package com.pactera.yhl.flinkck.job;

import com.pactera.yhl.flinkck.entity.FAgent;
import com.pactera.yhl.flinkck.entity.FPolicy;
import com.pactera.yhl.flinkck.map.FAgentMap;
import com.pactera.yhl.flinkck.map.FPolicyMap;
import com.pactera.yhl.flinkck.sink.FAgentSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class JobDefine {
    public static void jobFAgentDefine(StreamExecutionEnvironment env, String topic, Properties properties){
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromTimestamp(899999);
        SingleOutputStreamOperator<FAgent> source = env.addSource(kafkaConsumer).map(new FAgentMap());
        FAgentSink.FAgentSink(source);
    }
    public static void jobFPolicyDefine(StreamExecutionEnvironment env, String topic, Properties properties){
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromTimestamp(899999);
        SingleOutputStreamOperator<FPolicy> source = env.addSource(kafkaConsumer)
                .map(new FPolicyMap());

    }
}
