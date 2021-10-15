package com.pactera.yhl.insurance_detail.job.tmp_job;

import com.pactera.yhl.entity.Lbcont;
import com.pactera.yhl.entity.Lbpol;
import com.pactera.yhl.entity.Lccont;
import com.pactera.yhl.entity.Lcpol;
import com.pactera.yhl.insurance_detail.map.MapFuncTableAnychatcont;
import com.pactera.yhl.insurance_detail.process.filter.TmpLcpolFilter;
import com.pactera.yhl.insurance_detail.process.map.TmpLcpolMap;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class JobLuContDefine {
    public static void lucont_from_lccont(StreamExecutionEnvironment env, Properties properties, String topic,String kafkaGroupId) throws Exception {
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof Lccont);
//                .addSink(new TmpLbcontSink());
        env.execute("TmpLccontSink");
    }
    public static void lucont_from_lbcont(StreamExecutionEnvironment env, Properties properties, String topic,String kafkaGroupId) throws Exception {
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof Lbcont);
//                .addSink(new TmpLbcontSink());
        env.execute("TmpLbcontSink");
    }
}
