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
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;

public class InsertHbaseOnlyV2LBbak<T> extends RichSinkFunction<T> {
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

    public InsertHbaseOnlyV2LBbak(String tableName, String topic){
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
    @Override
    public void invoke(T value, Context context) throws Exception {
//        System.out.println("value = " + value);
//        Field columnName = value.getClass().getField("columnName");
//        String[] strs = columnName.get(value).toString().split(";");
//        String hbaseColumns = strs[0];
//        String column = strs[1];
//        String rowkeys = strs[2];
//        System.out.println("hbaseColumns = "+ hbaseColumns);
//        System.out.println("column = " + column);
//        System.out.println("rowkeys = " + rowkeys);
//        StringBuffer sb = new StringBuffer();
//        for(String rowkeyfield:rowkeys.split(",")){
//            Field field = value.getClass().getField(rowkeyfield);
//            System.out.println("field = " + field);
//            String s;
//            try {
//                s = field.get(value).toString();
//            } catch (Exception e){
//                continue;
//            }
//            sb.append(",");
//            sb.append(s);
//        }
//
//        Field columnField = value.getClass().getField(column);
//        String lbValue = columnField.get(value).toString();
//
//        BigDecimal b2 = new BigDecimal(lbValue);
//
//        if(hbaseColumns.contains(",")){
//            for(String hbaseColumn:hbaseColumns.split(",")){
//                Get get = new Get(Bytes.toBytes(sb.toString().substring(1,sb.toString().length())));
//                get.addColumn(Bytes.toBytes("f"),Bytes.toBytes(hbaseColumn));
//                Result result = table.get(get);
//                if(result.getValue(Bytes.toBytes("f"), Bytes.toBytes(column)) != null){
//                    byte[] fs = result.getValue(Bytes.toBytes("f"), Bytes.toBytes(column));
//                    if(fs.length == 0) continue;
//                    BigDecimal b1 = new BigDecimal(new String(fs));
////                    Double newValue = b1.subtract(b2,new MathContext(2)).doubleValue();
//                    String newValue = b1.subtract(b2,new MathContext(2)).toString();
//                    Put put = new Put(Bytes.toBytes(sb.toString().substring(1,sb.toString().length())));
//                    put.addColumn(Bytes.toBytes("f"),Bytes.toBytes(hbaseColumn),Bytes.toBytes(newValue));
//
//                    setNewDoubleValue(hbaseColumn,value,Double.valueOf(newValue));
//                }
//            }
//        }else{
//            Get get = new Get(Bytes.toBytes(sb.toString()));
//            get.addColumn(Bytes.toBytes("f"),Bytes.toBytes(hbaseColumns));
//            Result result = table.get(get);
//            if(result.getValue(Bytes.toBytes("f"), Bytes.toBytes(column)) != null){
//                byte[] fs = result.getValue(Bytes.toBytes("f"), Bytes.toBytes(column));
//                if(fs.length == 0 ) return;
//                BigDecimal b1 = new BigDecimal(new String(fs));
////                Double newValue = b1.subtract(b2,new MathContext(2)).doubleValue();
//                String newValue = b1.subtract(b2,new MathContext(2)).toString();
//
//                Put put = new Put(Bytes.toBytes(sb.toString()));
//                put.addColumn(Bytes.toBytes("f"),Bytes.toBytes(hbaseColumns),Bytes.toBytes(newValue));
//
//                setNewDoubleValue(hbaseColumns,value,Double.valueOf(newValue));
//            }
//        }
//        String jsonStr = genEntityJSON(value,
//                sb.toString().substring(1, sb.toString().length()), table);
//        insertKafka(jsonStr);
        insertKafka(value.toString());
    }


    private void insertKafka(String s){
        try{
            if(Objects.isNull(s)) return;
            producer.send(new ProducerRecord<>(topic,s));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private String genEntityJSON(T value,String rowkey,HTable hTable) throws Exception {
        System.out.println("before value = " + value);
        System.out.println("true rowkey = "+rowkey);
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = hTable.get(get);
        List<Cell> cells = result.listCells();
        if(Objects.isNull(cells)) return null;
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
        return JSON.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.WriteDateUseDateFormat);
    }

    private void setNewDoubleValue(String fieleName,T value,Double newValue) throws Exception {
        String methodName = "set"+ Util.LargerFirstChar(fieleName);
        Method method = value.getClass().getDeclaredMethod(methodName, Double.class);
        method.invoke(value,newValue);
    }





















//    private void insertClickhouse(T value) throws Exception {
//        //开始插入到clickhouse
//        Field[] declaredFields = value.getClass().getDeclaredFields();
//        StringBuffer sbKey = new StringBuffer();
//        StringBuffer sbValue = new StringBuffer();
//        Set<String> fields = new HashSet<>();
//        for(Field f:declaredFields){
//            if(f.getName().equals("fieldName")){
//                for(String fieldName:f.get(value).toString().split(",")){
//                    if(fieldName.equals("columnName") ||
//                            fieldName.equals("fieldName") ||
//                            fieldName.equals("valueField")){
//                        continue;
//                    }
//                    fields.add(fieldName);
//                }
//            }
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
