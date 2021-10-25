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
 * @create: 2021/10/14 0014 下午 16:07
 * @Desc:
 *
 * lup
 * left join ods.o_lis_LCInsured ic2 --险种被保人表(地址、电话)
 * on luc.contno=ic2.contno  and luc.source_id='0' --承保有效
 * and  lup.insuredno=ic2.insuredno
 *
 * lup ic2
 * left join o_lis_LCAddress add3  --险种被保人表(地址、电话)
 *  on ic2.AppntNo = add3.CustomerNo
 *  and ic2.AddressNo = add3.AddressNo
 */
public class FP1Add3_Ic2_LucLup extends AbstractJoin<Lcaddress, KL_lupol>{
    //中间表对象
    private AsyncTable<AdvancedScanResultConsumer> lucTable;
    private AsyncTable<AdvancedScanResultConsumer> lupTable;
    private AsyncTable<AdvancedScanResultConsumer> ic2Table;
    //宽表对象
    private AsyncTable<AdvancedScanResultConsumer> kuanbiao;

    @Override
    public void genTableConnection(CompletableFuture conn, Configuration hbaseConfig) throws Exception{
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            lucTable = connection.getTable(TableName.valueOf("KL:LUCONT_CONTNO"));
            lupTable = connection.getTable(TableName.valueOf("KL:LUPOL_INSUREDNO"));
            ic2Table = connection.getTable(TableName.valueOf("KL:LCINSURED_APPNTNO_ADDRESS"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_POLICY"));
        }
    }
    @Override
    public void asyncHandler(Lcaddress add3) throws Exception {

        String customerno = add3.getCustomerno();
        String addressno = add3.getAddressno();
        //json数据
        String add3Value = JSON.toJSONString(add3, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);
        // on ic2.AppntNo = add3.CustomerNo
        // and ic2.AddressNo = add3.AddressN
        Result ic2Result = getHbaseResult(customerno + addressno, ic2Table);
        if (!ic2Result.isEmpty()) {
            for (Cell listCell : ic2Result.listCells()) {
                String Value = Bytes.toString(CellUtil.cloneValue(listCell));
                JSONObject jsonObject = JSON.parseObject(Value);
                String ic2Contno = jsonObject.getString("contno");
                String ic2Insuredno = jsonObject.getString("insuredno");
                //on luc.contno=ic2.contno  and luc.source_id='0' --承保有效
                Result lucResult = getHbaseResult(ic2Contno, lucTable);
                if (!lucResult.isEmpty()) {
                    for (Cell luclistCell : lucResult.listCells()) {
                        String ycc = Bytes.toString(CellUtil.cloneValue(luclistCell));
                        JSONObject jsonObject1 = JSON.parseObject(ycc);
                        String source_id = jsonObject1.getString("source_id");

                        if ("0".equals(source_id)) {
                            //and  lup.insuredno=ic2.insuredno
                            Result lupResult = getHbaseResult(ic2Insuredno, lupTable);
                            if (!lupResult.isEmpty()) {
                                for (Cell cell : lupResult.listCells()) {
                                    String lupValue = Bytes.toString(CellUtil.cloneValue(cell));
                                    //获取宽表主键
                                    String rowKey = JSON.parseObject(lupValue).getString("polno");
                                    Put put = new Put(Bytes.toBytes(rowKey));
                                    put.addColumn(cf, Bytes.toBytes("fp1_lcaddress_3" + customerno + addressno), Bytes.toBytes(add3Value));
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
