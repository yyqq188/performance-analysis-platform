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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class InsertHbaseOnlyV2<T> extends RichSinkFunction<T> {
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

    public InsertHbaseOnlyV2(String tableName,String topic){
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

        //连接到clickhouse
//        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
//        sqlconnection = DriverManager.getConnection(address,user,password);
//        statement = sqlconnection.createStatement();

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
    @Override
    public void invoke(T value, Context context) throws Exception {
        Field columnName = value.getClass().getField("columnName");
        String[] strs = columnName.get(value).toString().split(";");
        String columns = strs[0];
        String rowkeys = strs[1];
        StringBuffer sb = new StringBuffer();
        for(String rowkeyfield:rowkeys.split(",")){
            Field field = value.getClass().getField(rowkeyfield);
            String s = field.get(value).toString();
            sb.append(",");
            sb.append(s);
        }
        List<Put> puts = new ArrayList<>();
        for(String column:columns.split(",")){
            Field columField = value.getClass().getField(column);
//            double columValue = Double.parseDouble(columField.get(value).toString());
            String columValue = columField.get(value).toString();
            Put put = new Put(Bytes.toBytes(sb.toString().substring(1,sb.toString().length())));
            put.addColumn(Bytes.toBytes("f"),Bytes.toBytes(column),Bytes.toBytes(columValue));
            puts.add(put);
        }
        table.put(puts);

        String tmp1 = sb.toString();
        String tmp2 = tmp1.substring(1, tmp1.length());
        String jsonstr = genEntityJSON(value,tmp2, table);
        insertKafka(jsonstr);
    }
    private void insertKafka(String s){
        try{
            producer.send(new ProducerRecord<>(topic,s));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private String genEntityJSON(T value,String rowkey,HTable hTable) throws Exception {
        System.out.println("before value = " + value);
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = hTable.get(get);
        List<Cell> cells = result.listCells();
        for(Cell cell:cells){
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            double hbaseValue = Double.valueOf(new String(CellUtil.cloneValue(cell)));
            setNewDoubleValue(qualifier,value,hbaseValue);
        }
        JSONObject jsonObject = new JSONObject();
        for(Field f:value.getClass().getDeclaredFields()){
            if("columnName".equals(f.getName())){
                continue;
            }
            jsonObject.put(f.getName(),f.get(value));
        }
        System.out.println("after value = " + value);
        return JSON.toJSONString(jsonObject,SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.WriteDateUseDateFormat);
    }

    private void setNewDoubleValue(String fieleName,T value,Double newValue) throws Exception {
        String methodName = "set"+ Util.LargerFirstChar(fieleName);
        Method method = value.getClass().getDeclaredMethod(methodName, Double.class);
        method.invoke(value,newValue);
    }

    private T setNewStringValue(String fieleName,T value,String newValue) throws Exception {
        String methodName = "set"+ Util.LargerFirstChar(fieleName);
        Method method = value.getClass().getDeclaredMethod(methodName, String.class);
        method.invoke(value,Util.toString(newValue));
        return value;
    }


    private T JSON2Entity(String jsonStr,T value) throws Exception {
        System.out.println("value = " + value);
        T newValue = setNewStringValue("key_id", value, "1111");
        return newValue;
    }







//    private Object JSON2Entity(String jsonStr,T value) throws Exception {
//        System.out.println("value = " + value);
////        T o = (T) JSON.parseObject(jsonStr, value.getClass());
//
//        setNewStringValue();
//        return o;
//    }

//    private T JSON2Entity(String jsonStr,T value) throws Exception {
//        //开始插入到clickhouse
//        Field[] declaredFields = value.getClass().getDeclaredFields();
//        Set<String> fields = new HashSet<>();
//        System.out.println("fields is == "+fields.toString());
//        JSONObject jsonObject = JSON.parseObject(jsonStr);
//        Set<String> jsonKeysets = jsonObject.keySet();
//        for(Field f:declaredFields){
//            Object o = f.get(value);
//            if(jsonKeysets.contains(f.getName())){
//                if("String".equals(f.getType().getSimpleName())){
//                    try{
//                        setNewStringValue(f.getName(),value,o.toString());
//                    }catch (Exception e){
//                        setNewStringValue(f.getName(),value,null);
//                    }
//                }else if("Double".equals(f.getType().getSimpleName())){
//                    try{
//                        setNewDoubleValue(f.getName(),value,Double.valueOf(o.toString()));
//                    }catch (Exception e){
//                        setNewDoubleValue(f.getName(),value,0.00);
//                    }
//                }
//            }
//        }
//        return value;
//    }


//    private void insertClickhouse(T value) throws Exception {
//        System.out.println("value " + value);
//        //开始插入到clickhouse
//        Field[] declaredFields = value.getClass().getDeclaredFields();
//        StringBuffer sbKey = new StringBuffer();
//        StringBuffer sbValue = new StringBuffer();
//        Set<String> fields = new HashSet<>();
//        for(Field f:declaredFields){
//            String fieldName = f.getName();
//            if(fieldName.equals("columnName") ||
//                    fieldName.equals("fieldName") ||
//                    fieldName.equals("valueField")){
//                continue;
//            }
//            fields.add(fieldName);
//        }
//        System.out.println("fields is == "+fields.toString());
//        for(Field f:declaredFields){
//            Object o = f.get(value);
//            if(fields.contains(f.getName())){
//                sbKey.append(",");
//                sbValue.append(",");
//                if("String".equals(f.getType().getSimpleName())){
//                    try{
//                        sbKey.append(f.getName());
//                        sbValue.append("\'" +o.toString()+"\'");
//
//                    }catch (Exception e){
//                        sbValue.append("\'null\'");
//                    }
//                }else if("Double".equals(f.getType().getSimpleName())){
//                    try{
//                        sbKey.append(f.getName());
//                        sbValue.append(Double.valueOf(o.toString()));
//                    }catch (Exception e){
//                        sbValue.append(0.00);
//                    }
//                }
//            }
//        }
//        String sql = String.format("insert into %s (%s) values (%s)",ckTableName,
//                sbKey.toString().toUpperCase().substring(1,sbKey.toString().length()),
//                sbValue.toString().substring(1,sbValue.toString().length()));
//        statement.executeQuery(sql);
//    }

}
