package com.pactera.yhl.insurance_detail.join;

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
 * @create: 2021/10/14 0014 下午 13:28
 * @Desc:
 *
 *  luc
 *  left join ods.o_lis_LCAppnt hc1 --投保人表(地址、电话)
 *  on hc1.contno=luc.contno and luc.source_id='0' --承保有效
 *
 *  luc hc1
 *  left join o_lis_LCAddress add1  --投保人表(地址、电话)
 *  on hc1.AppntNo = add1.CustomerNo
 *  and hc1.AddressNo = add1.AddressNo
 */
public class FP1Add1_Hc1_Luc extends AbstractJoin<Lcaddress, KL_lupol>{
    //中间表对象
    private AsyncTable<AdvancedScanResultConsumer> lucTable;
    private AsyncTable<AdvancedScanResultConsumer> hc1Table;
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
            hc1Table = connection.getTable(TableName.valueOf("KL:LCAPPNT_APPNTNO_ADDRESSNO"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_POLICY"));
        }
    }

    @Override
    public void asyncHandler(Lcaddress add1) throws Exception {

        //customerno + addressno
        String customerno = add1.getCustomerno();
        String addressno = add1.getAddressno();

        String add1Value = JSON.toJSONString(add1, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);
        //联合主键关联hc1
        Result hc1Result = getHbaseResult(customerno + addressno, hc1Table);
        if(!hc1Result.isEmpty()) {
            for (Cell listCell : hc1Result.listCells()) {
                String hc1Value = Bytes.toString(CellUtil.cloneValue(listCell));
                JSONObject jsonObject = JSON.parseObject(hc1Value);
                String hc1Contno = jsonObject.getString("contno");

                Result lucResult = getHbaseResult(hc1Contno, lucTable);
                if(!lucResult.isEmpty()) {
                    //获取luc数据
                    for (Cell cell : lucResult.listCells()) {
                        String lucValue = Bytes.toString(CellUtil.cloneValue(cell));
                        JSONObject ycc = JSON.parseObject(lucValue);
                        String lucSource_id = ycc.getString("source_id");
                        String lucContno = ycc.getString("contno");
                        //luc.source_id='0' --承保有效
                        if ("0".equals(lucSource_id)) {

                            Result lupResult = getHbaseResult(lucContno, lupTable);
                            if(!lupResult.isEmpty()){
                                for (Cell listCell1 : lupResult.listCells()) {
                                    String lupValue = Bytes.toString(CellUtil.cloneValue(listCell1));
                                    JSONObject jsonObject1 = JSON.parseObject(lupValue);
                                    String polno = jsonObject1.getString("polno");
                                    //获取宽表主键
                                    Put put = new Put(Bytes.toBytes(polno));
                                    put.addColumn(cf, Bytes.toBytes("fp1_lcaddress_1" + customerno + addressno), Bytes.toBytes(add1Value));
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
