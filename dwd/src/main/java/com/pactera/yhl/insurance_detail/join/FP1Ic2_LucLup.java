package com.pactera.yhl.insurance_detail.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.KL_lupol;
import com.pactera.yhl.entity.Lcinsured;
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
 * @create: 2021/10/14 0014 下午 15:05
 * @Desc:
 *
 *
 * lup
 * left join ods.o_lis_LCInsured ic2 --险种被保人表(地址、电话)
 * on luc.contno=ic2.contno  and luc.source_id='0' --承保有效
 * and  lup.insuredno=ic2.insuredno
 */
public class FP1Ic2_LucLup extends AbstractJoin<Lcinsured, KL_lupol>{
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
            lupTable = connection.getTable(TableName.valueOf("KL:LUPOL_INSUREDNO"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_POLICY"));
        }
    }

    @Override
    public void asyncHandler(Lcinsured ic2) throws Exception {

        String contno = ic2.getContno();
        String insuredno = ic2.getInsuredno();
        //json数据
        String ic2Value = JSON.toJSONString(ic2, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);
        //on luc.contno=ic2.contno  and luc.source_id='0' --承保有效
        Result lucResult = getHbaseResult(contno, lucTable);
        if (!lucResult.isEmpty()) {
            for (Cell listCell : lucResult.listCells()) {
                String Value = Bytes.toString(CellUtil.cloneValue(listCell));
                JSONObject jsonObject = JSON.parseObject(Value);
                String source_id = jsonObject.getString("source_id");

                if ("0".equals(source_id)) {
                    //and  lup.insuredno=ic2.insuredno
                    Result lupResult = getHbaseResult(insuredno, lupTable);
                    if (!lupResult.isEmpty()) {
                        for (Cell cell : lupResult.listCells()) {
                            String lupValue = Bytes.toString(CellUtil.cloneValue(cell));
                            //获取宽表主键
                            String rowKey = JSON.parseObject(lupValue).getString("polno");
                            Put put = new Put(Bytes.toBytes(rowKey));
                            put.addColumn(cf, Bytes.toBytes("fp2_lcinsured_2" + contno + insuredno), Bytes.toBytes(ic2Value));
                            kuanbiao.put(put);
                        }
                    }
                }
            }
        }
    }
}
