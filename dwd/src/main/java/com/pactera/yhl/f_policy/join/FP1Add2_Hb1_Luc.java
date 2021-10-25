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
 * @create: 2021/10/14 0014 下午 14:50
 * @Desc:
 *
 * luc
 * left join ods.o_lis_LBAppnt hb1 --退保投保人表(地址、电话)退保
 * on hb1.contno=luc.contno  and  luc.source_id='1' --退保
 *
 * luc hb1
 * left join o_lis_LCAddress add2  --投保人表(地址、电话)
 *  on hb1.AppntNo = add2.CustomerNo
 *  and hb1.AddressNo = add2.AddressNo
 */
public class FP1Add2_Hb1_Luc extends AbstractJoin<Lcaddress, KL_lupol>{
    //中间表对象
    private AsyncTable<AdvancedScanResultConsumer> lucTable;
    private AsyncTable<AdvancedScanResultConsumer> lupTable;
    private AsyncTable<AdvancedScanResultConsumer> hb1Table;
    //宽表对象
    private AsyncTable<AdvancedScanResultConsumer> kuanbiao;

    @Override
    public void genTableConnection(CompletableFuture conn, Configuration hbaseConfig) throws Exception{
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            lucTable = connection.getTable(TableName.valueOf("KL:LUCONT_CONTNO"));
            lupTable = connection.getTable(TableName.valueOf("KL:LUPOL_CONTNO"));
            hb1Table = connection.getTable(TableName.valueOf("KL:LBAPPNT_APPNTNO_ADDRESSNO"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_POLICY"));
        }
    }

    @Override
    public void asyncHandler(Lcaddress add2) throws Exception {

        //customerno + addressno
        String customerno = add2.getCustomerno();
        String addressno = add2.getAddressno();

        String add2Value = JSON.toJSONString(add2, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);
        //联合主键关联hb1
        Result hc1Result = getHbaseResult(customerno + addressno, hb1Table);
        if (!hc1Result.isEmpty()) {
            for (Cell listCell : hc1Result.listCells()) {
                String hb1Value = Bytes.toString(CellUtil.cloneValue(listCell));
                JSONObject jsonObject = JSON.parseObject(hb1Value);
                String hb1Contno = jsonObject.getString("contno");

                Result lucResult = getHbaseResult(hb1Contno, lucTable);
                if (!lucResult.isEmpty()) {
                    //获取luc数据
                    for (Cell cell : lucResult.listCells()) {
                        String lucValue = Bytes.toString(CellUtil.cloneValue(cell));
                        JSONObject jsonObject2 = JSON.parseObject(lucValue);
                        String lucSource_id = jsonObject2.getString("source_id");
                        String lucContno = jsonObject2.getString("contno");
                        //luc.source_id='0' --承保有效
                        if ("1".equals(lucSource_id)) {

                            Result lupResult = getHbaseResult(lucContno, lupTable);
                            if(!lupResult.isEmpty()){
                                for (Cell listCell1 : lupResult.listCells()) {
                                    String lupValue = Bytes.toString(CellUtil.cloneValue(listCell1));
                                    JSONObject lupJson = JSON.parseObject(lupValue);
                                    String lupPolno = lupJson.getString("polno");
                                    Put put = new Put(Bytes.toBytes(lupPolno));
                                    put.addColumn(cf, Bytes.toBytes("fp1_lcaddress_2" + customerno + addressno), Bytes.toBytes(add2Value));
                                    kuanbiao.put(put);

                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
