package com.pactera.yhl.sink.abstr;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kudu.client.*;

/**
 * mode形式：
 * SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND 后台自动一次性批处理刷新提交N条数据
 * SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC  每次自动同步刷新提交每条数据
 * SessionConfiguration.FlushMode.MANUAL_FLUSH 	手动刷新一次性提交N条数据
 */
public abstract class AbstractKuduSink<IN> extends RichSinkFunction<IN> {

    public String tableName;
    public KuduClient client;
    public KuduTable table;
    public KuduSession session;

    @Override
    public void open(Configuration parameters) throws Exception {
        String kuduIP = "prod-bigdata-pc9";
        long timeOut = 60000L;
        int cacheNum = 10000;
        client = new KuduClient.KuduClientBuilder(kuduIP)
                .defaultAdminOperationTimeoutMs(timeOut).build();
        table = client.openTable(tableName);
        session = client.newSession();
        session.setTimeoutMillis(timeOut);
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH); //mode形式
        session.setMutationBufferSpace(cacheNum);// 缓冲大小，也就是数据的条数


    }

    @Override
    public void close() throws Exception {
        session.close();
        client.close();

    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        handler(value, context);
    }
    public abstract void handler(IN value, Context context) throws Exception;
}
