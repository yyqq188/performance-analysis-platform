package com.pactera.yhl.insurance_detail.job.tmp_job;

import com.pactera.yhl.entity.*;
import com.pactera.yhl.insurance_detail.map.MapFuncTableAnychatcont;
import com.pactera.yhl.insurance_detail.sink.TmpMidLbcontSink;
import com.pactera.yhl.insurance_detail.sink.TmpMidLccontSink;
import com.pactera.yhl.insurance_detail.sink.TmpMidLcphoinfonewresultSink;
import com.pactera.yhl.insurance_detail.sink.TmpMidLjfeeclassSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class JobMidLuContDefine {
    public static void lucont_from_lccont(StreamExecutionEnvironment env, Properties properties, String topic,String kafkaGroupId) throws Exception {
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof Lccont)
                .map(x -> (Lccont) x)
                .addSink(new TmpMidLccontSink());
        env.execute("TmpLccontSink");
    }
    public static void lucont_from_lbcont(StreamExecutionEnvironment env, Properties properties, String topic,String kafkaGroupId) throws Exception {
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof Lbcont)
                .map(x -> (Lbcont) x)
                .addSink(new TmpMidLbcontSink());
        env.execute("TmpLbcontSink");
    }

    public static void lccont_rownum(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId) throws Exception {
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof Ljtempfeeclass)
                .map(x -> (Ljtempfeeclass) x)
                .addSink(new TmpMidLjfeeclassSink());
        env.execute("TmpLbcontSink");
    }

    public static void lccont_infonewresult(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId) throws Exception {
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof Lcphoinfonewresult)
                .map(x -> (Lcphoinfonewresult) x)
                .addSink(new TmpMidLcphoinfonewresultSink());
        env.execute("TmpLbcontSink");
    }
}
