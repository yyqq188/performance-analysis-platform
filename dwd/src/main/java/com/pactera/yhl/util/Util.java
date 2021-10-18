package com.pactera.yhl.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pactera.yhl.entity.KLEntity;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Util {
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

    // 同时往hbase和kafka插入数据
    public static void insertHbaseAndKafka(String rowkey, KLEntity value, String columnName, HTable hTable,
                                           KafkaProducer<String,String> producer, String topic) throws Exception {
        Put put = new Put(Bytes.toBytes(rowkey));
        String valueJson = JSON.toJSONString(value, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);
        put.addColumn(cf,Bytes.toBytes(columnName),Bytes.toBytes(valueJson));
        hTable.put(put);

        producer.send(new ProducerRecord<>(topic,valueJson));
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
