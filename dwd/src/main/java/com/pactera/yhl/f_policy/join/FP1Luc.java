package com.pactera.yhl.f_policy.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.KL_lucont;
import com.pactera.yhl.entity.KL_lupol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.pactera.yhl.util.Util.getHbaseResult;

/**
 * @description:
 * @author: TSY
 * @create: 2021/10/14 0014 上午 11:09
 * @Desc:
 */
public class FP1Luc extends AbstractJoin<KL_lucont, KL_lupol>{
    //宽表对象
    private AsyncTable<AdvancedScanResultConsumer> lupTable;
    private AsyncTable<AdvancedScanResultConsumer> kuanbiao;

    @Override
    public void genTableConnection(CompletableFuture conn, Configuration hbaseConfig) throws Exception{
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            lupTable = connection.getTable(TableName.valueOf("KL:LUPOL_CONTNO"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_POLICY"));
        }
    }

    @Override
    public void asyncHandler(KL_lucont luc) throws Exception {
        String contno = luc.getContno();

        String lucValue = JSON.toJSONString(luc, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);

        Result lupResult = getHbaseResult(contno, lupTable);
        if(!lupResult.isEmpty()){
            for (Cell listCell : lupResult.listCells()) {
                String Value = Bytes.toString(CellUtil.cloneValue(listCell));
                JSONObject jsonObject = JSON.parseObject(Value);
                String polno = jsonObject.getString("polno");
                Put put = new Put(Bytes.toBytes(polno));
                put.addColumn(cf, Bytes.toBytes("lucont" + contno), Bytes.toBytes(lucValue));
                kuanbiao.put(put);
            }
        }
    }
}
