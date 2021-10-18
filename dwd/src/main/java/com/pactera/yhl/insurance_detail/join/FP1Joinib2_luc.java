package com.pactera.yhl.insurance_detail.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.KL_lucont;
import com.pactera.yhl.entity.Lbinsured;
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
 * @Description
 * @create 2021/10/15 9:59
 */
public class FP1Joinib2_luc extends AbstractJoin<Lbinsured, KL_lucont>{
    //中间表对象
    private AsyncTable<AdvancedScanResultConsumer> lucTable;
    private AsyncTable<AdvancedScanResultConsumer> lupTable;
    //宽表对象
    private AsyncTable<AdvancedScanResultConsumer> kuanbiao;
    @Override
    public void genTableConnection(CompletableFuture<AsyncConnection> conn, Configuration hbaseConfig) throws Exception {
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            lucTable = connection.getTable(TableName.valueOf("KL:LUCONT_CONTNO"));
            lupTable = connection.getTable(TableName.valueOf("KL:LUPOL_CONTNO"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_POLICY"));
        }
    }
//    left join ods.o_lis_LBInsured ib2 --险种被保人表(地址、电话)
//    on luc.contno=ib2.contno  and luc.source_id='1' --退保
    @Override
    public void asyncHandler(Lbinsured ib2) throws Exception {
        //获取ib2主键及关联字段
        String contno = ib2.getContno();
        String insuredno = ib2.getInsuredno();
        //插入宽表信息
        String ib2Value = JSON.toJSONString(ib2, SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.WriteDateUseDateFormat);
        //通过关联条件获取luc表信息
        Result lucResult = getHbaseResult(contno + "", this.lucTable);
        if(!lucResult.isEmpty()){
            for (Cell lucListCell : lucResult.listCells()){
                String lucValue = Bytes.toString(CellUtil.cloneValue(lucListCell));
                JSONObject lucjsonObject = JSON.parseObject(lucValue);
                String lucContno = lucjsonObject.getString("contno");
                //通过关联条件获取lup表信息
                if("1".equals( lucjsonObject.getString("source_id"))){
                    Result lupResult = getHbaseResult(lucContno + "", this.lupTable);
                    if(!lupResult.isEmpty()){
                        for(Cell lupListCell : lupResult.listCells()){
                            String lupValue = Bytes.toString(CellUtil.cloneValue(lupListCell));
                            JSONObject lupjsonObject = JSON.parseObject(lupValue);
                            String rowKey = lupjsonObject.getString("polno");
                            Put put = new Put(Bytes.toBytes(rowKey));
                            put.addColumn(cf,Bytes.toBytes("fp1_lbinsured_2" + contno + insuredno),
                                    Bytes.toBytes(ib2Value));
                            kuanbiao.put(put);
                        }
                    }
                }
            }
        }
    }

}
