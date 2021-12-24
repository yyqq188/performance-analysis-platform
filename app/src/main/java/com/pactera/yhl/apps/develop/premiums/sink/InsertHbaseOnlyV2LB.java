package com.pactera.yhl.apps.develop.premiums.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.sink.abstr.MyHbaseCli;
import com.pactera.yhl.sink.abstr.MyKafka;
import com.pactera.yhl.util.Util;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class InsertHbaseOnlyV2LB<T> extends RichSinkFunction<T> {
    protected String rowkey;
    protected static final String cfString = "f";
    protected static final byte[] cf = Bytes.toBytes(cfString);
    protected  String tableName;
    private Connection connection;
    protected HTable table;

//    String address = "jdbc:clickhouse://10.5.2.134:8123/default";
//    String user = "default";
//    String password = "default";
//    java.sql.Connection sqlconnection;
//    Statement statement;
//    String ckTableName;

    //kafka
    protected String topic ;
    protected KafkaProducer<String,String> producer;

    public InsertHbaseOnlyV2LB(String tableName, String topic){
        this.tableName = tableName;
        this.topic = topic;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool params = (ParameterTool) getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();
        String config_path = params.get("config_path");
        connection = MyHbaseCli.hbaseConnection(config_path);
        table = (HTable) connection.getTable(TableName.valueOf(tableName));

        //kafka的配置
        final Properties props = MyKafka.getProperties(config_path);
        Properties kafkaProps = new Properties();
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("bootstrap.servers",props.getProperty("kafka_bootstrap_servers"));
        producer=new KafkaProducer<String, String>(kafkaProps);

    }
    @Override
    public void close() throws Exception {
        connection.close();
    }
    //prem_day,approve_prem_day;hesi_prem_day;key_id,day_id,manage_code,manage_name
    @Override
    public void invoke(T value, Context context) throws Exception {
        System.out.println("value = " + value);
        Field columnName = value.getClass().getField("columnName");
        String[] strs = columnName.get(value).toString().split(";");
        String driveSubField = strs[1];
        String rowkeys = strs[2];
        StringBuffer sb = new StringBuffer();
        for(String rowkeyfield:rowkeys.split(",")){
            Field field = value.getClass().getField(rowkeyfield);
            String s;
            try {
                s = field.get(value).toString();
            } catch (Exception e){
                continue;
            }
            sb.append(",");
            sb.append(s);
        }
        //rowkey字符串
        //这是直接发送到hbase
        String rowkeyStr = sb.toString().substring(1,sb.toString().length());
        Field driveSubFieldF = value.getClass().getField(driveSubField);
        String driveSub = driveSubFieldF.get(value).toString();
        Put put = new Put(Bytes.toBytes(rowkeyStr));
        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes(driveSubField),Bytes.toBytes(driveSub));
        table.put(put);

        //这是直接发送到kafka
        insertKafka(genEntityJSON(value));
    }

    private void insertKafka(String s){
        try{
            if(Objects.isNull(s)) return;
            producer.send(new ProducerRecord<>(topic,s));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private String genEntityJSON(T value) throws Exception {

        JSONObject jsonObject = new JSONObject();
        for(Field f:value.getClass().getDeclaredFields()){
//            if("columnName".equals(f.getName())){
//                continue;
//            }
            jsonObject.put(f.getName(),f.get(value));
        }
        return JSON.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.WriteDateUseDateFormat);
    }
}
