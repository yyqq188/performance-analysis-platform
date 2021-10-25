package com.pactera.yhl.f_agent.job;

import com.pactera.yhl.entity.T02salesinfo_k;
import com.pactera.yhl.f_agent.map.MapFuncTableAnychatcont;
import com.pactera.yhl.f_agent.process.filter.TmpOXTOFilter;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Map;
import java.util.Properties;

/**
 * @author: TSY
 * @create: 2021/10/20 0020 上午 10:17
 * @description:
 */
public class JobDefine {


    public static void t02salesinfo_kInsert(
            StreamExecutionEnvironment env,
            Properties properties, String topic, String kafkaGroupId) throws Exception {

        Map<String, String> configMap = env.getConfig().getGlobalJobParameters().toMap();
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof T02salesinfo_k)
                .map(x -> (T02salesinfo_k) x)
                .filter(new TmpOXTOFilter()) //过滤形成lup中lbp的on中条件
                .map(x -> x.toString())
                .addSink(new FlinkKafkaProducer<String>(configMap.get("kafka_bootstrap_servers"),
                        configMap.get("kafka_topic_t02salesinfo_k"), new SimpleStringSchema()));
        env.execute("TmpT02salesinfo_kSink");
    }

}
