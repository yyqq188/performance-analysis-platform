package com.pactera.yhl.insurance_detail.job.tmp_job;

import com.pactera.yhl.entity.*;
import com.pactera.yhl.insurance_detail.map.MapFuncTableAnychatcont;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

public class JobPreLuContDefine {
    public static void lccont_pre(StreamExecutionEnvironment env, Properties properties, String topic, String kafkaGroupId) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        properties.setProperty("group.id", kafkaGroupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        SingleOutputStreamOperator<KLEntity> ljtempfeeclass = source.map(new MapFuncTableAnychatcont())
                .filter(x -> x != null)
                .filter(x -> x instanceof Ljtempfeeclass);
        tableEnv.createTemporaryView("ljtempfeeclass",ljtempfeeclass);
        Table table = tableEnv.sqlQuery("select confmakedate,otherno,row_number() over " +
                "(partition by confmakedate,otherno order by confmakedate asc) as conf from ljtempfeeclass");
        DataStream<String> stringDataStream = tableEnv.toAppendStream(table, String.class);
        stringDataStream.addSink(new FlinkKafkaProducer<String>("10.5.2.133:6667,10.5.2.134:6667,10.5.2.144:6667,10.5.2.145:6667",
                "tmptopic", new SimpleStringSchema()));

        env.execute("TmpLcpolSink");
    }
}

