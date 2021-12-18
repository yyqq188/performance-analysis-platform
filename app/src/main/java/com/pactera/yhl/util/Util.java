package com.pactera.yhl.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pactera.yhl.entity.source.KLEntity;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class Util {

    /**@Create Sun Haitian */
    //通过 rowkey column获得值
    public static Result getHbaseValue(HTable hTable,String rowkey,String family,
                                       String columnName) throws IOException {
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(
                Bytes.toBytes(family),
                Bytes.toBytes(columnName));
        Result values = hTable.get(get);
        return values;

    }
    public static final byte[] cf = Bytes.toBytes("f");
    //字符串转可执行程序
    public static Object convertToCode(String jexlExp, Map<String,Object> map){
        JexlEngine jexl=new JexlEngine();
        Expression e = jexl.createExpression(jexlExp);
        JexlContext jc = new MapContext();
        for(String key:map.keySet()){
            jc.set(key, map.get(key));
        }
        if(null==e.evaluate(jc)){
            return "";
        }
        return e.evaluate(jc);
    }


    public static <T> Tuple2<List<String>,List<String>> getHbaseValue(Object o, Class<T> clazz,
                                                                      String[] rowkeys, String[] columns,
                                                                      String tableName, AsyncTable<AdvancedScanResultConsumer> table) throws Exception {
        ObjectMapper objMapper = new ObjectMapper();
        T t = objMapper.convertValue(o, clazz);
        String rowkey = String.join("", rowkeys);
        String column2 = String.join("", columns);
        String column = tableName+column2;

        byte[] cf = Bytes.toBytes("f");
        Get get = new Get(Bytes.toBytes(rowkey));
        String valueJson = JSON.toJSONString(t, SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);
        get.addColumn(cf, Bytes.toBytes(column));

        CompletableFuture<Result> resultCompletableFuture = table.get(get);
        Result result = resultCompletableFuture.get();
        List<String> resultRowKeys = new ArrayList<>();
        List<String> resultColumns = new ArrayList<>();
        for(Cell cell:result.listCells()){
            String qualifierName = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            resultRowKeys.add(value);
            resultColumns.add(qualifierName);
        }
        return new Tuple2<List<String>,List<String>>(resultRowKeys,resultColumns);

    }


    public static Result getHbaseResult(String rowkey,AsyncTable<AdvancedScanResultConsumer> table) throws Exception{
        String chdrcoy = rowkey;
        Get get = new Get(Bytes.toBytes(chdrcoy+""));
        get.addFamily(cf);
        CompletableFuture<Result> resultCompletableFuture = table.get(get);
        Result result = resultCompletableFuture.get();
        return result;
    }
    public static Result getHbaseResultSync(String rowkey,HTable table) throws Exception{
        String chdrcoy = rowkey;
        Get get = new Get(Bytes.toBytes(chdrcoy+""));
        get.addFamily(cf);
        Result result = table.get(get);
        return result;
    }

    // 同时往hbase和kafka插入数据
    public static void insertHbaseAndKafka(String rowkey, KLEntity value, String columnName, HTable hTable,
                                           KafkaProducer<String,String> producer, String topic) throws Exception {
        Put put = new Put(Bytes.toBytes(rowkey));
        String valueJson = JSON.toJSONString(value, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);
        put.addColumn(cf,Bytes.toBytes(columnName),Bytes.toBytes(valueJson));
        hTable.put(put);

        producer.send(new ProducerRecord<>(topic,valueJson));
    }

    //首字母大写
    public static String LargerFirstChar(String str) {
        char[] chars = str.toCharArray();
        chars[0] -= 32;
        if(chars[0]>97){

        }
        return String.valueOf(chars);
    }


    //判断某个kafka的topic是否存在
    public static boolean topicExists(String kafkaservers,String topic){
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaservers);
        AdminClient adminClient = AdminClient.create(prop);
        ArrayList<NewTopic> topics = new ArrayList<>();
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        try{
            Map<String, TopicListing> maps = listTopicsResult.namesToListings().get();
            if(maps.containsKey(topic)){
                return true;
            }else{
                return false;
            }

        }catch (Exception e){
            e.printStackTrace();
        }

        return false;

    }
    //创建新的topic
    public static boolean createTopic(String kafkaservers,String topic){
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaservers);
        AdminClient adminClient = AdminClient.create(prop);
        ArrayList<NewTopic> topics = new ArrayList<>();
        NewTopic newTopic = new NewTopic(topic,1,(short) 1);
        topics.add(newTopic);
        CreateTopicsResult result = adminClient.createTopics(topics);
        try{
            result.all().get();
            return true;
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }


    //hbase通过rowkey,列名
    public static void insertHbaseValue(HTable hTable,String rowkey,String family,
                                        String columnName,String value) throws IOException {
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(
                Bytes.toBytes(family),
                Bytes.toBytes(columnName),
                Bytes.toBytes(value));
        hTable.put(put);

    }
    //通过 表名 rowkey column获得值
    public static List<String> getHbaseValueWithTableNames
    (HTable hTable,String rowkey,String family,String columnName) throws IOException {
        List<String> values = new ArrayList<>();
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(
                Bytes.toBytes(family),
                Bytes.toBytes(columnName));
        Result result = hTable.get(get);
        for(Cell cell:result.listCells()) {
            byte[] bytes = CellUtil.cloneValue(cell);
            String value = Bytes.toString(bytes);
            values.add(value);
        }
        return values;
    }
    //通过表名,rowkey 获得全部column的json
    public static String getHbaseJsonValue(
            HTable hTable,String rowkey,String family,String columnName) throws IOException {
        String value = "";
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(
                Bytes.toBytes(family),
                Bytes.toBytes(columnName));
        Result result = hTable.get(get);
        return Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(columnName)));
    }

    public static String getHbaseColumnNameValue(
            HTable hTable,String rowkey,String family,String columnName) throws IOException {
        String value = "";
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(
                Bytes.toBytes(family),
                Bytes.toBytes(columnName));
        Result result = hTable.get(get);
        return Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(columnName)));
    }
    //通过表名,rowkey 获得指定字段的值
    public static String getHbaseValue(
            HTable hTable,String rowkey,String family,
            String columnName,String field) throws IOException {
        Map<String,String> valueMaps = new HashMap<>();
        String json = "";
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addFamily(Bytes.toBytes(family));
        Result result = hTable.get(get);
        if(result.value() == null) return  "";
        String resultValue = "";
        for(Cell cell:result.listCells()) {
            byte[] bytes = CellUtil.cloneValue(cell);
            byte[] qualifierBytes = CellUtil.cloneQualifier(cell);
            String value = Bytes.toString(bytes);
            String qualifier = Bytes.toString(qualifierBytes);

            if(qualifier.equals(columnName)){
                resultValue = JSON.parseObject(value).getString(field);
            }
        }
        return resultValue;
    }


    //toString遇到null时会报错，所以这里给null一个空字符串
    public static String toString(Object o){
        try{
            return o.toString();
        }catch (Exception e){
            e.printStackTrace();
            return "";
        }
    }


    public static String toSubString(Object o){
        try{
            String s = o.toString();
            return s.substring(2,s.length());
        }catch (Exception e){
            e.printStackTrace();
            return "";
        }
    }

    public static void main(String[] args) {
//        System.out.println(LargerFirstChar("esadaaaa"));
        String kafkaservers = "10.5.2.133:6667,10.5.2.134:6667,10.5.2.144:6667,10.5.2.145:6667";
        String topic = "testyhlv3";
        boolean exists = topicExists(kafkaservers, topic);
        System.out.println(exists);
        boolean testyhlv4 = createTopic(kafkaservers, "testyhlv4");
        System.out.println(testyhlv4);


    }
}


//    public static void tmp(){
//        StringBuilder rowkeySb = new StringBuilder();
//        StringBuilder columnSb = new StringBuilder();
//        columnSb.append(tableName);
//        try {
//            Map<String, Object> map = new HashMap<String, Object>();
//
//            for (String rowkey : rowkeys) {
//                map.put("value", t);
//                map.put("rowkey", rowkey);
//                String expression = "value.getStateflag(rowkey)";
//                Object value1 = Util.convertToCode(expression, map);
//                rowkeySb.append(value1);
//            }
//            if (columns.length > 0) {
//                for (String rowkey : columns) {
//                    map.put("value", t);
//                    map.put("rowkey", rowkey);
//                    String expression = "value.getStateflag(rowkey)";
//                    Object value1 = Util.convertToCode(expression, map);
//                    columnSb.append(value1);
//                }
//            }
//        }catch (Exception e){
//
//
//
//}






//判断某个kafka的topic是否存在
//    public static boolean topicExists_bak(String zkConnection,String topic){
//        ZkUtils zkUtils = ZkUtils.apply(zkConnection, 30000, 30000, JaasUtils.isZkSecurityEnabled());
//        boolean exists = AdminUtils.topicExists(zkUtils, topic);
//        return exists;
//    }
//    //创建新的topic
//    public static boolean createTopic_bak(String zkConnection,String topic){
//        ZkUtils zkUtils = ZkUtils.apply(zkConnection, 30000, 30000, JaasUtils.isZkSecurityEnabled());
//        try{
//            AdminUtils.createTopic(zkUtils,topic,1,1,new Properties(), RackAwareMode.Enforced$.MODULE$);
//            return true;
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        return false;
//    }

