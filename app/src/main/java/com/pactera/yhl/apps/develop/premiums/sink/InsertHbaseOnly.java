package com.pactera.yhl.apps.develop.premiums.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.sink.abstr.MyHbaseCli;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;

public class InsertHbaseOnly<T> extends RichSinkFunction<T> {
    protected String rowkey;
    protected static final String cfString = "f";
    protected static final byte[] cf = Bytes.toBytes(cfString);
    protected  String tableName;
    private Connection connection;
    protected HTable table;
    public InsertHbaseOnly(String tableName){
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        ParameterTool params = (ParameterTool) getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();
        String config_path = params.get("config_path");
        connection = MyHbaseCli.hbaseConnection(config_path);
        table = (HTable) connection.getTable(TableName.valueOf(tableName));
    }
    @Override
    public void close() throws Exception {
        connection.close();
    }
    @Override
    public void invoke(T value, Context context) throws Exception {
        Field key_id = value.getClass().getField("key_id");
        String rowkey = key_id.get(value).toString();
        Field columnName = value.getClass().getField("columnName");
        String column = columnName.get(value).toString();

        Put put = new Put(Bytes.toBytes(rowkey));
        String valueJson = JSON.toJSONString(value, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);
        put.addColumn(cf, Bytes.toBytes(column), Bytes.toBytes(valueJson));
        table.put(put);
    }
}
