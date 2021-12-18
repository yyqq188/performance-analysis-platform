package com.pactera.yhl.apps.measure.sink;

import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.measure.entity.FactWageBase;
import com.pactera.yhl.sink.abstr.MyHbaseCli;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class InsertHbaseSink extends RichSinkFunction<FactWageBase> {
    public static String cfString = "f";
    public static byte[] cf = Bytes.toBytes(cfString);

    public static Connection connection;
    public static HTable testTable;
    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String config_path = params.get("config_path");
        connection = MyHbaseCli.hbaseConnection(config_path);
        try{
            //Todo 查询 APPLICATION_ASSESSMENT_PERSION_RESULT 表
            testTable = (HTable) connection.getTable(TableName.valueOf("KLMIDAPP:ASSESSRESULTTEST"));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public void invoke(FactWageBase value, Context context) throws Exception {
        String key_id = value.getKey_id();
        Put put = new Put(Bytes.toBytes(key_id));
        put.addColumn(cf, Bytes.toBytes(key_id), Bytes.toBytes(JSONObject.toJSONString(value)));
        testTable.put(put);
    }
}
