package com.pactera.yhl.apps.develop.premiums.premise.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity02;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity03;
import com.pactera.yhl.apps.develop.premiums.job.ComplexLogic;
import com.pactera.yhl.entity.source.Lcpol;
import com.pactera.yhl.sink.abstr.MyHbaseCli;
import com.pactera.yhl.sink.abstr.MyKafka;
import com.pactera.yhl.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.*;

public class JoinInsertKafkaSHT extends RichSinkFunction<PremiumsKafkaEntity02> {
    protected static final String cfString = "f";
    protected static final byte[] cf = Bytes.toBytes(cfString);
    protected  String tableName;
    private Connection connection;
    protected KafkaProducer<String,String> producer;
    protected String topicOut ;
    protected String topicInt ;

    protected Map<String,String> joinFieldsDriver;
    protected Set<String> otherFieldsDriver;
    protected Set<String> fieldsHbase;
    protected Class<?> hbaseClazz;
    protected Class<?> kafkaClazz;
    protected Map<String,String> filterMapDriver;
    protected Map<String,String> filterMapHbase;



    public JoinInsertKafkaSHT(String tableName, String topicInt,String topicOut) throws Exception {
        this.tableName = tableName;//HBase中间表名
        this.topicOut = topicOut;
        this.topicInt = topicInt;
    }



    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String config_path = params.get("config_path");

//        String config_path = "D:\\Users\\Desktop\\pactera\\code_project\\performance-analysis-platform\\app\\src\\main\\resources\\configuration.properties";
        connection = MyHbaseCli.hbaseConnection(config_path);

        //kafka的配置
        final Properties props = MyKafka.getProperties(config_path);
        Properties kafkaProps = new Properties();
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("bootstrap.servers",props.getProperty("kafka_bootstrap_servers"));

        producer=new KafkaProducer<String, String>(kafkaProps);

        //创建目的中间的topic
        String kafkaservers = props.getProperty("kafka_bootstrap_servers");
        boolean isExists = Util.topicExists(kafkaservers,topicOut);
        if(!isExists){
            Util.createTopic(kafkaservers,topicOut);
        }

        boolean isExistsInt = Util.topicExists(kafkaservers,topicInt);
        if(!isExistsInt){
            Util.createTopic(kafkaservers,topicInt);
        }


    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public void invoke(PremiumsKafkaEntity02 value, Context context) throws Exception {
        HTable hTable = null;
        try{
            hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
            handle(value,context,hTable);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void handle(PremiumsKafkaEntity02 premiumsKafkaEntity02, Context context, HTable hTable) throws Exception {
        System.out.println("handle ing ....");

        HTable midHbase =(HTable) connection.getTable(TableName.valueOf("KLMIDAPPRUN:productcode_intv_payyears"));
        ComplexLogic.complexLogic(premiumsKafkaEntity02,topicInt,topicOut,hTable,midHbase,producer);


    }
}
