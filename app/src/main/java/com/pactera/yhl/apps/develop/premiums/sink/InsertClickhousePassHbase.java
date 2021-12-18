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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class InsertClickhousePassHbase<T> extends RichSinkFunction<T> {
    String address = "jdbc:clickhouse://10.5.2.134:8123/default";
    String user = "default";
    String password = "default";
    Connection connection;
    Statement statement;
    ResultSet results;
    String sql;
    String tableName;

    private org.apache.hadoop.hbase.client.Connection HbaseConnection;
    protected HTable hbaseTable;
    protected String hbaseTableName;

    public InsertClickhousePassHbase(String tableName,String hbaseTableName){
        this.tableName = tableName;
        this.hbaseTableName = hbaseTableName;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        connection = DriverManager.getConnection(address,user,password);
        statement = connection.createStatement();


        //连接hbase
        ParameterTool params = (ParameterTool) getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();
        String config_path = params.get("config_path");
        HbaseConnection = MyHbaseCli.hbaseConnection(config_path);
        hbaseTable = (HTable) HbaseConnection.getTable(TableName.valueOf(hbaseTableName));
    }

    @Override
    public void close() throws Exception {

        if(results!=null){
            results.close();
        }
        if(statement!=null){
            statement.close();
        }
        if(connection!=null){
            connection.close();
        }
    }

    @Override
    public void invoke(T value, Context context) throws Exception {

        Field key_id = value.getClass().getField("key_id");
        String rowkey = key_id.get(value).toString();
        System.out.println("rowkey " + rowkey);
        Field columnName = value.getClass().getField("columnName");
        String columnStr = columnName.get(value).toString();
        System.out.println("columnStr " + columnStr);

        Field valueField = value.getClass().getField("valueField");
        String valueFieldStr = valueField.get(value).toString();
        String hbaseField = valueFieldStr.split(",")[0];
        String messageField = valueFieldStr.split(",")[1];
        String assignmentField = valueFieldStr.split(",")[2];
        System.out.println("hbaseField " + hbaseField);
        System.out.println("messageField " + messageField);



        //获得hbase中的数据
        Result result = hbaseTable.get(new Get(Bytes.toBytes(rowkey)));
        if(result.getValue(Bytes.toBytes("f"),Bytes.toBytes(columnStr)) == null)  return;
        byte[] valuefs = result.getValue(Bytes.toBytes("f"), Bytes.toBytes(columnStr));
        String cellValue = new String(valuefs);
        JSONObject jsonObject = JSON.parseObject(cellValue);
        Object o1 = jsonObject.get(hbaseField);
        Double hbasePreDouble = Double.valueOf(o1.toString());
        System.out.println("hbasePreDouble " + hbasePreDouble);

        Field suffValue = value.getClass().getField(messageField);
        System.out.println("suffValue field " + suffValue);
        Double suffValueDouble = Double.valueOf(suffValue.get(value).toString());
        System.out.println("suffValueDouble "+ suffValueDouble);

        BigDecimal b1 = new BigDecimal(hbasePreDouble);
        BigDecimal b2 = new BigDecimal(suffValueDouble);

        String methodName = "set"+Util.LargerFirstChar(assignmentField);
        Method method = value.getClass().getDeclaredMethod(methodName, Double.class);
        method.invoke(value,b1.subtract(b2,new MathContext(2)).doubleValue());


        //开始插入到clickhouse
        Field[] declaredFields = value.getClass().getDeclaredFields();
        StringBuffer sbKey = new StringBuffer();
        StringBuffer sbValue = new StringBuffer();
        Set<String> fields = new HashSet<>();
        for(Field f:declaredFields){
            if(f.getName().equals("fieldName")){
                for(String fieldName:f.get(value).toString().split(",")){
                    if(fieldName.equals("columnName") ||
                            fieldName.equals("fieldName") ||
                            fieldName.equals("valueField")){
                        continue;
                    }
                    fields.add(fieldName);
                }
            }
        }
        System.out.println("fields is == "+fields.toString());
        for(Field f:declaredFields){
            Object o = f.get(value);
            if(fields.contains(f.getName())){
                sbKey.append(",");
                sbValue.append(",");
                if("String".equals(f.getType().getSimpleName())){
                    try{
                        sbKey.append(f.getName());
                        sbValue.append("\'" +o.toString()+"\'");

                    }catch (Exception e){
                        sbValue.append("\'null\'");
                    }
                }else if("Double".equals(f.getType().getSimpleName())){
                    try{
                        sbKey.append(f.getName());
                        sbValue.append(Double.valueOf(o.toString()));
                    }catch (Exception e){
                        sbValue.append(0.00);
                    }
                }
            }
        }
        sql = String.format("insert into %s (%s) values (%s)",tableName,
                sbKey.toString().toUpperCase().substring(1,sbKey.toString().length()),
                sbValue.toString().substring(1,sbValue.toString().length()));
        statement.executeQuery(sql);
    }
}