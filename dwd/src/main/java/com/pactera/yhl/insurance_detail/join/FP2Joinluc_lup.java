package com.pactera.yhl.insurance_detail.join;

import com.alibaba.fastjson.JSON;
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
 * @author Sun Haitian
 * @Description 接收 KL_LUCONT luc, 插入luc表数据
 * @create 2021/10/14 17:36
 */
public class FP2Joinluc_lup extends AbstractJoin<KL_lucont, KL_lupol>{
    //中间表对象
    private AsyncTable<AdvancedScanResultConsumer> lupTable;
    //宽表对象
    private AsyncTable<AdvancedScanResultConsumer> kuanbiao;
    @Override
    public void genTableConnection(CompletableFuture<AsyncConnection> conn, Configuration hbaseConfig) throws Exception {
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            lupTable = connection.getTable(TableName.valueOf("KL:LUPOL_PRTNO"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_POLICY"));
        }
    }
//    inner join KL_LUCONT luc
//    on luc.prtno=lup.prtno
    @Override
    public void asyncHandler(KL_lucont luc) throws Exception {
        //获取luc主键及关联字段
        String prtno = luc.getPrtno();
        //插入宽表信息
        String lucValue = JSON.toJSONString(luc, SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.WriteDateUseDateFormat);
        //通过关联条件获取lup表信息
        Result lupResult = getHbaseResult(prtno + "", this.lupTable);
        if(!lupResult.isEmpty()){
            for (Cell lupListCell : lupResult.listCells()){
                String lupValue = Bytes.toString(CellUtil.cloneValue(lupListCell));
                String rowKey = JSON.parseObject(lupValue).getString("polno");
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(cf,Bytes.toBytes("fp2_kl_lucont" + prtno),
                        Bytes.toBytes(lucValue));
                kuanbiao.put(put);
            }
        }
    }

}
