package com.pactera.yhl.sink.abstr;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class AbstractHbaseSink<IN> extends RichSinkFunction<IN> {

    protected static final String cfString = "f";
    protected static final byte[] cf = Bytes.toBytes(cfString);
    protected  String tableName = null;
    private Connection connection;
    @Override
    public void open(Configuration parameters) throws Exception {
        String configPath = "D:\\Users\\Desktop\\pactera\\code_project\\performance-analysis-platform\\app\\src\\main\\resources\\configuration.properties";
        connection = MyHbaseCli.hbaseConnection(configPath);
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
        handle(value,context,table);
    }



    public abstract void handle(IN value, Context context,HTable table) throws Exception;
}
