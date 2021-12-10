package com.pactera.yhl.apps.develop.premiums.sink;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.sink.abstr.MyHbaseCli;
import com.pactera.yhl.sink.abstr.MyKafka;
import com.pactera.yhl.util.Util;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.*;

public class InsertKafkaOnly<OUT> extends RichSinkFunction<OUT> {
    protected String topic ;
    protected KafkaProducer<String,String> producer;
    public InsertKafkaOnly(String topic){
        this.topic = topic; //"testyhlv3";  //目的topic
    }



    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String config_path = params.get("config_path");


        //kafka的配置
        final Properties props = MyKafka.getProperties(config_path);
        Properties kafkaProps = new Properties();
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("bootstrap.servers",props.getProperty("kafka_bootstrap_servers"));

        producer=new KafkaProducer<String, String>(kafkaProps);

        //创建目的中间的topic
        String kafkaservers = props.getProperty("kafka_bootstrap_servers");
        boolean isExists = Util.topicExists(kafkaservers,topic);
        if(!isExists){
            Util.createTopic(kafkaservers,topic);
        }


    }

    @Override
    public void close() throws Exception {
        producer.close();
    }

    @Override
    public void invoke(OUT value, Context context) throws Exception {

        try{
            producer.send(new ProducerRecord<>(topic,
                    JSON.toJSONString(value)));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
