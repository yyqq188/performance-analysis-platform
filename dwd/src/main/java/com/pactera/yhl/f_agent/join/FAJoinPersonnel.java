package com.pactera.yhl.f_agent.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.T02salesinfo_k;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author Sun Haitian
 * @Description F_Agent 人员表
 * @create 2021/10/20 10:26
 */
public class FAJoinPersonnel extends AbstractJoin<T02salesinfo_k, T02salesinfo_k>{
    //宽表对象
    private AsyncTable<AdvancedScanResultConsumer> kuanbiao;

    @Override
    public void genTableConnection(CompletableFuture<AsyncConnection> conn, Configuration hbaseConfig) throws Exception {
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_AGENT"));
        }
    }
//    ODS.O_XG_T02SALESINFO_K OXTO
    @Override
    public void asyncHandler(T02salesinfo_k t02salesinfo_k) throws Exception {
        String sales_id = t02salesinfo_k.getSales_id();
        String t02Value = JSON.toJSONString(t02salesinfo_k, SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);
        Put put = new Put(Bytes.toBytes(sales_id));
        put.addColumn(cf, Bytes.toBytes("t02salesinfo_k" + sales_id), Bytes.toBytes(t02Value));
        kuanbiao.put(put);
    }
}
