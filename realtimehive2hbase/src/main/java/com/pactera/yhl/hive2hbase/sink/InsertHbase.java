package com.pactera.yhl.hive2hbase.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

public  class InsertHbase extends RichSinkFunction<String> {
    protected static final String cfString = "f";
    protected static final byte[] cf = Bytes.toBytes(cfString);
    protected  String tableName = null;
    protected Connection connection;
    protected SimpleDateFormat sdf;
    protected String[] rowkeys;


    HTable hTable = null;

    public InsertHbase(String tableName,String[] rowkeys){
        this.tableName = tableName;
        this.rowkeys = rowkeys;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        sdf = new SimpleDateFormat("yyyy-MM-dd");

        String zkUrl = params.get("zkquorum");
        String zkPort = params.get("zkport");
        connection = MyHbaseCli.hbaseConnection(zkUrl,zkPort);

        try{
            hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
//            //无则创建表
//            createTable(hTable);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        try{
            handle(value,context,hTable);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public  void handle(String value, Context context,HTable hTable) throws Exception{

        try{
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(new Date());
            calendar.add(Calendar.DATE,-1);
            Date preDate = calendar.getTime();
            String preDateStr = sdf.format(preDate);

//            String rowkey = preDateStr + "#" + String.join("",rowkeys);
            //todo 这里根据业务要修改
            String rowkey = String.join("",rowkeys);


            JSONObject jsonObject = JSON.parseObject(value);
            JSONObject data = jsonObject.getJSONObject("data");
            JSONObject meta = jsonObject.getJSONObject("meta");
            String tableName = meta.getString("tableName");
            JSONArray fields = meta.getJSONArray("fields");


            Put put = null;
            put = new Put(data.get(rowkey).toString().getBytes(StandardCharsets.UTF_8));
            for (int i = 0; i < fields.size(); i++) {
                String field = fields.get(i).toString();
                put.addColumn("f".getBytes(StandardCharsets.UTF_8),
                        field.getBytes(StandardCharsets.UTF_8),
                        data.get(field).toString().getBytes(StandardCharsets.UTF_8));

            }
            hTable.put(put);

        }catch (Exception e){
            e.printStackTrace();
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
