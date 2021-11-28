package com.pactera.yhl.apps.develop.premiums.premise.mid;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.sink.abstr.MyHbaseCli;
import com.pactera.yhl.util.Util;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 用于处理order操作
 */
public class InsertHbaseOrder<OUT> extends RichSinkFunction<OUT> {
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
    HTable hTable = null;
    //用于order的字段
    protected String[] orderColumns;
    //order的顺序
    protected String order;
    //hbase数据的类型
    Class<?> clazz;
    public InsertHbaseOrder(String tableName,String[] rowkeys,String[] columnNames,Class<?> clazz,
                            String[] orderColumns,String order,
                            String columnTableName){

        this.tableName = tableName;
        this.rowkeys = rowkeys;
        this.columnNames = columnNames;
        this.columnTableName = columnTableName;
        this.orderColumns = orderColumns;
        this.order = order;
        this.clazz = clazz;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();


        String config_path = params.get("config_path");
//        String config_path = "D:\\Users\\Desktop\\pactera\\code_project\\performance-analysis-platform\\app\\src\\main\\resources\\configuration.properties";
        connection = MyHbaseCli.hbaseConnection(config_path);

        try{
            hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
            //无则创建表
            createTable(hTable);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public void invoke(OUT value, Context context) throws Exception {
        try{
            handle(value,context,hTable);
            System.out.println(value);
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
                Field field = value.getClass().getField(rowkey);
                rowkeySb.append(field.get(value).toString());
            }

            if(columnNames.length > 0 ) {
                for(String rowkey:columnNames){
                    Field field = value.getClass().getField(rowkey);
                    columnSb.append(field.get(value));
                }
            }
            Get get = new Get(Bytes.toBytes(rowkeySb.toString()));
            get.addColumn(cf,Bytes.toBytes(columnSb.toString()));
            Result result = hTable.get(get);
            if(result != null){
                List<Cell> columnCells = result.getColumnCells(cf, Bytes.toBytes(columnSb.toString()));
                for(Cell cell:columnCells){
                    String cellValue = new String(CellUtil.cloneValue(cell));
                    JSONObject jsonObject = JSON.parseObject(cellValue);

                    for(String colum:columnNames){
                        Double hbaseVal = Double.valueOf(jsonObject.get(colum).toString());
                        Field field = value.getClass().getField(colum);
                        Double messageVal = Double.valueOf(field.get(value).toString());
                        if(order.equals("desc")){
                            if(messageVal > hbaseVal){
                                String methodName = "set"+ Util.LargerFirstChar(colum);
                                Method method = clazz.getDeclaredMethod(methodName, String.class);
                                method.invoke(clazz.newInstance(),String.valueOf(messageVal));
                            }

                        }else if(order.equals("asc")){
                            if(messageVal < hbaseVal){
                                String methodName = "set"+ Util.LargerFirstChar(colum);
                                Method method = clazz.getDeclaredMethod(methodName, String.class);
                                method.invoke(clazz.newInstance(),String.valueOf(messageVal));
                            }

                        }
                    }


                }
            }


            Put put = new Put(Bytes.toBytes(rowkeySb.toString()));
            String valueJson = JSON.toJSONString(value, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);
            put.addColumn(cf, Bytes.toBytes(columnSb.toString()), Bytes.toBytes(valueJson));
            hTable.put(put);

        }catch (Exception e){
            System.out.println(e);
        }
    }



    public void createTable(HTable table) throws IOException {
        Admin admin = connection.getAdmin();
        boolean b = admin.tableExists(TableName.valueOf(tableName));
        if(b) return;


        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                ColumnFamilyDescriptorBuilder.newBuilder(cf);
        columnFamilyDescriptorBuilder.setBloomFilterType(BloomType.ROW);
        columnFamilyDescriptorBuilder.setMaxVersions(1);

        ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

        try{
            admin.createTable(tableDescriptor);

        }catch (Exception e){
            System.out.println("已经创建表  "+tableName);
        }finally {
            admin.close();
        }
    }

}
