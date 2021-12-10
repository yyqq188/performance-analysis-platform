package com.pactera.yhl.flinkdemo;

import com.pactera.yhl.join.TableInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


import java.util.Properties;

public class RegisterSourceTable {
    public static void registerSourceTable(TableInfo tableInfo,
                                           StreamTableEnvironment tbEnv,
                                           StreamExecutionEnvironment env){
        Properties propertie = tableInfo.getProps();
        String tableName = tableInfo.getTableName();

        if("kafke".equals(propertie.getProperty("type"))){
            Properties kafkaprops = new Properties();
            propertie.forEach((k,v)->{
                if(k.toString().startsWith("kafka.")){
                    kafkaprops.setProperty(k.toString().replace("kafka.",""),v.toString());

                }
            });
            String topic = propertie.getProperty("kafka.topic");
//            FlinkKafkaConsumer<Row> consumer = new FlinkKafkaConsumer<Row>(topic,new JsonDeserialization(
//                    tableInfo.getFieldInfo().keySet(),tableInfo.getFieldInfo().values(),kafkaprops));
//
//            DataStreamSource<Row> source = env.addSource(consumer);
//            tbEnv.registerTable(tableName,source);


        }
    }

}
