package com.pactera.yhl.insurance_detail.sink;

import lombok.Data;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class AbstractInsertHbase<KLEntiy> extends RichSinkFunction<KLEntiy> {
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
        String zkUrl = params.get("zookeeper");
        String zkPort = params.get("zk_port");
        String hbaseId= params.get("hbase_secureid");
        String hbaseKey= params.get("hbase_securekey");
        org.apache.hadoop.conf.Configuration hbaseConfig = HBaseConfiguration.create();

        hbaseConfig.set(HConstants.ZOOKEEPER_QUORUM,zkUrl);
        hbaseConfig.set(HConstants.ZOOKEEPER_CLIENT_PORT,zkPort);
        hbaseConfig.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,"30000");
        hbaseConfig.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,"30000");
        hbaseConfig.setLong("hbase.rpc.timeout", 600000);
        hbaseConfig.setLong("hbase.regionserver.lease.period", 600000);
        hbaseConfig.set("hbase.client.ipc.pool.type","ThreadLocalPool");
        hbaseConfig.set("hbase.client.ipc.pool.size","2");  //1
        if(null == connection){
            connection = ConnectionFactory.createConnection(hbaseConfig);
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public void invoke(KLEntiy value, Context context) throws Exception {
        HTable hTable = null;
        try{
            hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
            handle(value,context,hTable);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public abstract void handle(KLEntiy value, Context context,HTable hTable) throws Exception;
}