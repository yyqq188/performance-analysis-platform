package com.pactera.yhl.insurance_detail.join;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class  AbstractJoin<INT,OUT>  extends RichAsyncFunction<INT,OUT> {
    protected static final String cfString = "f";
    protected static final byte[] cf = Bytes.toBytes(cfString);
    protected  String tableName = null;
    public CompletableFuture<AsyncConnection> conn;
    public AsyncConnection connection;


    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String zkUrl = params.get("zookeeper");
        String zkPort = params.get("zk_port");

        org.apache.hadoop.conf.Configuration hbaseConfig = HBaseConfiguration.create();

        hbaseConfig.set( "zookeeper.znode.parent", "/hbase-unsecure");
        hbaseConfig.set(HConstants.ZOOKEEPER_QUORUM, zkUrl);
        hbaseConfig.set(HConstants.ZOOKEEPER_CLIENT_PORT, zkPort);
        hbaseConfig.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "600000");
        hbaseConfig.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "600000");
        hbaseConfig.setLong("hbase.rpc.timeout", 600000);
        hbaseConfig.setLong("hbase.regionserver.lease.period", 600000);
        hbaseConfig.set("hbase.client.ipc.pool.type","ThreadLocalPool");
        hbaseConfig.set("hbase.client.ipc.pool.size","1");  //1
        genTableConnection(conn,hbaseConfig);

    }
    @Override
    public void asyncInvoke(INT value, ResultFuture<OUT> resultFuture) throws Exception{
        asyncHandler(value);
    }
    public abstract void asyncHandler(INT value) throws Exception;
    public abstract void genTableConnection(CompletableFuture<AsyncConnection> conn,
                                            org.apache.hadoop.conf.Configuration hbaseConfig) throws Exception;

}