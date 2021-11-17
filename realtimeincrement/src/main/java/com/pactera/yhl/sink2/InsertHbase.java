package com.pactera.yhl.sink2;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.Util;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public  class InsertHbase<OUT> extends RichSinkFunction<OUT> {
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

    public InsertHbase(String tableName, String[] rowkeys, String[] columnNames, String columnTableName){

        this.tableName = tableName;
        this.rowkeys = rowkeys;
        this.columnNames = columnNames;
        this.columnTableName = columnTableName;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();


        String zkUrl = params.get("zkquorum");
        String zkPort = params.get("zkport");
        connection = MyHbaseCli.hbaseConnection(zkUrl,zkPort);

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
        List<Put> putLists = new ArrayList<Put>();
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
                    Put put = new Put(Bytes.toBytes(rowkeySb.toString()));
                    put.addColumn(cf,Bytes.toBytes(rowkey),Bytes.toBytes(value1.toString()));
                    putLists.add(put);
                }
            }

            hTable.put(putLists);

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
