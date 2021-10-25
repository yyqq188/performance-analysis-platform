package com.pactera.yhl.f_policy.job.tmp_job;

import com.pactera.yhl.entity.*;
import com.pactera.yhl.f_policy.map.MapFuncTableAnychatcont;
import com.pactera.yhl.f_policy.sink.TmpRowNumLjtempfeeclassSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class JobRowNumLjtempfeeclassDefine {
    public static void lccont_rownum(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        source.map(new MapFuncTableAnychatcont())
        .filter(x -> x != null)
        .filter(x -> x instanceof Ljtempfeeclass)
                .map( x -> (Ljtempfeeclass) x)
                .addSink(new TmpRowNumLjtempfeeclassSink());
        env.execute("TmpLcpolSink");
    }
}

