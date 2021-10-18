package com.pactera.yhl.insurance_detail.job.tmp_job;

import com.pactera.yhl.entity.Lbpol;
import com.pactera.yhl.entity.Ljtempfeeclass;
import com.pactera.yhl.insurance_detail.map.MapFuncTableAnychatcont;
import com.pactera.yhl.insurance_detail.process.filter.TmpLbpolFilter;
import com.pactera.yhl.insurance_detail.process.map.TmpLbpolMap;
import com.pactera.yhl.insurance_detail.sink.TmpRowNumLjtempfeeclassSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class JobLuContDefine {

    public static void lucont_from_lbcont(StreamExecutionEnvironment env, Properties properties, String topic,String kafkaGroupId) throws Exception {

        Map<String, String> configMap = env.getConfig().getGlobalJobParameters().toMap();
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
                .filter(x -> x != null)
                .filter(x -> x instanceof Lbpol)
                .map(x -> (Lbpol) x)
                .filter(new TmpLbpolFilter())
                .map(new TmpLbpolMap())
                .map(x -> x.toString())
                .addSink(new FlinkKafkaProducer<String>(configMap.get("kafka_bootstrap_servers"),
                        configMap.get("kafka_topic_lupol"), new SimpleStringSchema()));
        env.execute("TmpLbpolSink");
    }

    public static void lucont_from_lcont(StreamExecutionEnvironment env, Properties properties, String topic,String kafkaGroupId) throws Exception {

//        Map<String, String> configMap = env.getConfig().getGlobalJobParameters().toMap();
//        properties.setProperty("group.id", kafkaGroupId);
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
//        kafkaConsumer.setStartFromEarliest();
//        DataStreamSource<String> source = env.addSource(kafkaConsumer);
//        source.map(new MapFuncTableAnychatcont())  //.print();
//                .filter(x -> x != null)
//                .filter(x -> x instanceof Lbpol)
//                .map(x -> (Lbpol) x)
//                .filter(new TmpLbpolFilter())
//                .map(new TmpLbpolMap())
//                .map(x -> x.toString())
//                .addSink(new FlinkKafkaProducer<String>(configMap.get("kafka_bootstrap_servers"),
//                        configMap.get("kafka_topic_lupol"), new SimpleStringSchema()));
//        env.execute("TmpLbpolSink");



        Map<String, String> configMap = env.getConfig().getGlobalJobParameters().toMap();
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.map(new MapFuncTableAnychatcont())  //.print();
        .filter(x -> x != null);
//        AsyncDataStream.unorderedWait(source,new FP1Ic2_LucLup(),10, TimeUnit.SECONDS,10);
        env.execute("TmpLbpolSink");
    }

}
