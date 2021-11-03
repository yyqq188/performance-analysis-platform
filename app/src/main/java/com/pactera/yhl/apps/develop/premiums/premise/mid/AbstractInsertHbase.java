package com.pactera.yhl.apps.develop.premiums.premise.mid;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.sink.abstr.MyHbaseCli;
import com.pactera.yhl.util.Util;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractInsertHbase<OUT> extends RichSinkFunction<OUT> {
    protected static final String cfString = "f";
    protected static final byte[] cf = Bytes.toBytes(cfString);
    protected  String tableName = null;
    private Connection connection;

    //rowkey
    protected String[] rowkeys = {};
    //列名
    protected String[] columnNames = {};
    //固定的表名
    protected String columnTableName = "";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();


        String configPath = "D:\\Users\\Desktop\\pactera\\code_project\\performance-analysis-platform\\app\\src\\main\\resources\\configuration.properties";
        connection = MyHbaseCli.hbaseConnection(configPath);
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public void invoke(OUT value, Context context) throws Exception {
        HTable hTable = null;
        try{
            hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
            handle(value,context,hTable);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public  void handle(OUT value, Context context,HTable hTable) throws Exception{
        StringBuilder rowkeySb = new StringBuilder();
        StringBuilder columnSb = new StringBuilder();
        columnSb.append(columnTableName);
        try{
            Map<String,Object> map=new HashMap<String,Object>();
            for(String rowkey:rowkeys){
                map.put("value",value);
                String expression = "value.get"+ Util.LargerFirstChar(rowkey)+"()";
                Object value1 = Util.convertToCode(expression,map);
                rowkeySb.append(value1);
            }

            if(columnNames.length > 0 ) {
                for(String rowkey:columnNames){
                    map.put("value",value);
                    String expression = "value.get"+Util.LargerFirstChar(rowkey)+"()";
                    Object value1 = Util.convertToCode(expression,map);
                    columnSb.append(value1);
                }
            }
//            Put put = new Put(Bytes.toBytes(value.getChdrcoy() + value.getChdrnum()));
            Put put = new Put(Bytes.toBytes(rowkeySb.toString()));
            String valueJson = JSON.toJSONString(value, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);
            put.addColumn(cf, Bytes.toBytes(columnSb.toString()), Bytes.toBytes(valueJson));
            hTable.put(put);

        }catch (Exception e){

        }
        System.out.println("tableName is "+tableName);
    }
}