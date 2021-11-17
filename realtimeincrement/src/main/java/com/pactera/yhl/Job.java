package com.pactera.yhl;

import com.pactera.yhl.entity.Lbpol;
import com.pactera.yhl.entity.Ldcode;
import com.pactera.yhl.sink.InsertHive;
import com.pactera.yhl.sink.InsertHiveV2;
import com.pactera.yhl.sink2.InsertHbase;
import com.pactera.yhl.transfor.TestMapTransformFunc;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Job {
    public static void insertHiveTable1(StreamExecutionEnvironment env, String topic, Properties prop){
        prop.setProperty("group.id","JobPremiums_lbpol2saleinfo6");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Ldcode)
                .map(x -> (Ldcode) x)
//                .addSink(new InsertHive());
                .addSink(new InsertHiveV2());


    }
    public static void insertHbaseTable2(StreamExecutionEnvironment env,String topic ,Properties prop,
    String tableName, String[] rowkeys, String[] columnNames, String columnTableName){
        prop.setProperty("group.id","JobPremiums_lbpol2saleinfo6");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), prop);
        kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());
        env.addSource(kafkaConsumer)
                .map(new TestMapTransformFunc())
                .filter(x -> x instanceof Ldcode)
                .map(x -> (Ldcode) x)
                .addSink(new InsertHbase<>(tableName,rowkeys,columnNames,columnTableName));
    }
}
