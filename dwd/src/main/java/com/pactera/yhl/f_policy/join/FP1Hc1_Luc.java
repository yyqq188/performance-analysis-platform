package com.pactera.yhl.f_policy.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.KL_lupol;
import com.pactera.yhl.entity.Lcaddress;
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
 * @create: 2021/10/14 0014 下午 13:16
 * @Desc:left   join ods.o_lis_lcappnt hc1 --投保人表(地址、电话)
 *              on hc1.contno=luc.contno and luc.source_id='0' --承保有效
 */
public class FP1Hc1_Luc extends AbstractJoin<Lcaddress, KL_lupol>{
    //中间表对象
    private AsyncTable<AdvancedScanResultConsumer> lucTable;
    private AsyncTable<AdvancedScanResultConsumer> lupTable;
    //宽表对象
    private AsyncTable<AdvancedScanResultConsumer> kuanbiao;

    @Override
    public void genTableConnection(CompletableFuture conn, Configuration hbaseConfig) throws Exception{
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            lucTable = connection.getTable(TableName.valueOf("KL:LUCONT_CONTNO"));
            lupTable = connection.getTable(TableName.valueOf("KL:LUPOL_CONTNO"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_POLICY"));
        }
    }

    @Override
    public void asyncHandler(Lcaddress hc1) throws Exception {
//
////        String contno = hc1.getContno();
////        String hc1Value = JSON.toJSONString(hc1, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);
//
//        Result lucResult = getHbaseResult(contno, lucTable);
//        if (!lucResult.isEmpty()) {
//            for (Cell listCell : lucResult.listCells()) {
//                String lucValue = Bytes.toString(CellUtil.cloneValue(listCell));
//                JSONObject jsonObject = JSON.parseObject(lucValue);
//                String lucSource_id = jsonObject.getString("source_id");
//                String lucContno = jsonObject.getString("contno");
//
//                if ("0".equals(lucSource_id)) {
//                    //获取宽表主键
//                    Result lupResult = getHbaseResult(lucContno, lupTable);
//                    if(!lupResult.isEmpty()){
//                        for (Cell lupCell : lupResult.listCells()) {
//                            String lupValue = Bytes.toString(CellUtil.cloneValue(lupCell));
//                            JSONObject lupJson = JSON.parseObject(lupValue);
//                            String lupPolno = lupJson.getString("polno");
//                            Put put = new Put(Bytes.toBytes(lupPolno));
//                            put.addColumn(cf, Bytes.toBytes("fp1_lcappnt_1" + contno), Bytes.toBytes(hc1Value));
//                            kuanbiao.put(put);
//                        }
//                    }
//                }
//            }
//        }
    }
}
