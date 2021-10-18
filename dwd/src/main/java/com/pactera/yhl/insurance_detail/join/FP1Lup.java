package com.pactera.yhl.insurance_detail.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.KL_lupol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @author: TSY
 * @create: 2021/10/14 0014 上午 10:20
 * @Desc:
 */
public class FP1Lup extends AbstractJoin<KL_lupol, KL_lupol>{
    //宽表对象
    private AsyncTable<AdvancedScanResultConsumer> kuanbiao;

    @Override
    public void genTableConnection(CompletableFuture conn, Configuration hbaseConfig) throws Exception{
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_POLICY"));
        }
    }

    @Override
    public void asyncHandler(KL_lupol lup) throws Exception {
        String polno = lup.getPolno();
        String lupValue = JSON.toJSONString(lup, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);
        Put put = new Put(Bytes.toBytes(polno));
        put.addColumn(cf, Bytes.toBytes("lupol" + polno), Bytes.toBytes(lupValue));
        kuanbiao.put(put);
    }
}
