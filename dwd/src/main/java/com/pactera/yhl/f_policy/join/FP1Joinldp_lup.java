package com.pactera.yhl.f_policy.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.KL_lupol;
import com.pactera.yhl.entity.Ldplan;
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
 * @Description 接收 ODS.o_lis_ldplan LDP, 插入lup表数据, 取组合计划名称计划
 * @create 2021/10/14 9:43
 */
public class FP1Joinldp_lup extends AbstractJoin<Ldplan, KL_lupol>{
    //中间表对象
    private AsyncTable<AdvancedScanResultConsumer> lupTable;
    //宽表对象
    private AsyncTable<AdvancedScanResultConsumer> kuanbiao;
    @Override
    public void genTableConnection(CompletableFuture<AsyncConnection> conn, Configuration hbaseConfig) throws Exception {
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            lupTable = connection.getTable(TableName.valueOf("KL:LUPOL_CONTPLANCODE"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_POLICY"));
        }
    }

//    LEFT JOIN ODS.o_lis_ldplan LDP  --组合计划名称计划
//    ON LDP.contplancode2 = lup.contplancode
//    primarykey : contplancode, plantype, contplanname2, contplancode2
    @Override
    public void asyncHandler(Ldplan ldp) throws Exception {
        //获取ldp主键及关联字段
        String contplancode2 = ldp.getContplancode2();
        String contplancode = ldp.getContplancode();
        String plantype = ldp.getPlantype();
        String contplanname2 = ldp.getContplanname2();
        //插入宽表信息
        String ldpValue = JSON.toJSONString(ldp, SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.WriteDateUseDateFormat);
        //通过关联条件获取lup表信息
        Result lupResult = getHbaseResult(contplancode2 + "", this.lupTable);
        if (!lupResult.isEmpty()){
            for (Cell lupListCell : lupResult.listCells()){
                String lupValue = Bytes.toString(CellUtil.cloneValue(lupListCell));
                String rowKey = JSON.parseObject(lupValue).getString("polno");
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(cf,Bytes.toBytes("fp1_ldplan" + contplancode + plantype + contplanname2 + contplancode2),
                        Bytes.toBytes(ldpValue));
                kuanbiao.put(put);
            }
        }
    }

}
