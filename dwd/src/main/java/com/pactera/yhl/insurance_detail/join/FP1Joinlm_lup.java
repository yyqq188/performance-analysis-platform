package com.pactera.yhl.insurance_detail.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.KL_lupol;
import com.pactera.yhl.entity.Lmriskapp;
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
 * @Description ods.o_lis_LMRiskApp lm, 插入lup表数据, 取 险种信息
 * @create 2021/10/14 15:03
 */
public class FP1Joinlm_lup extends AbstractJoin<Lmriskapp, KL_lupol>{
    //中间表对象
    private AsyncTable<AdvancedScanResultConsumer> lupTable;
    //宽表对象
    private AsyncTable<AdvancedScanResultConsumer> kuanbiao;
    @Override
    public void genTableConnection(CompletableFuture<AsyncConnection> conn, Configuration hbaseConfig) throws Exception {
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            lupTable = connection.getTable(TableName.valueOf("KL:LUPOL_RISKCODE"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_POLICY"));
        }
    }
//    left  join ods.o_lis_LMRiskApp lm  --获取险种信息
//    on lm.RiskCode = lup.RiskCode
    @Override
    public void asyncHandler(Lmriskapp lm) throws Exception {
        //获取lac主键及关联字段
        String riskcode = lm.getRiskcode();
        //插入宽表信息
        String lmValue = JSON.toJSONString(lm, SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.WriteDateUseDateFormat);
        //通过关联条件获取lup表信息
        Result lupResult = getHbaseResult(riskcode + "", this.lupTable);
        if (!lupResult.isEmpty()){
            for (Cell lupListCell : lupResult.listCells()){
                String lupValue = Bytes.toString(CellUtil.cloneValue(lupListCell));
                String rowKey = JSON.parseObject(lupValue).getString("polno");
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(cf,Bytes.toBytes("fp1_lmriskapp" + riskcode),
                        Bytes.toBytes(lmValue));
                kuanbiao.put(put);
            }
        }
    }

}
