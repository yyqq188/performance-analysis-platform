package com.pactera.yhl.f_policy.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.KL_lupol;
import com.pactera.yhl.entity.Lbappnt;
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
 * @create: 2021/10/14 0014 下午 14:23
 * @Desc:   luc
 *          left join ods.o_lis_LBAppnt hb1 --退保投保人表(地址、电话)退保
 *          on hb1.contno=luc.contno  and  luc.source_id='1' --退保
 */
public class FP1Hb1_Luc extends AbstractJoin<Lbappnt, KL_lupol>{
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
    public void asyncHandler(Lbappnt hb1) throws Exception {
        //contno
        String contno = hb1.getContno();

        String hb1Value = JSON.toJSONString(hb1, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);

        Result lucResult = getHbaseResult(contno, lucTable);
        if (!lucResult.isEmpty()) {
            for (Cell listCell : lucResult.listCells()) {
                String Value = Bytes.toString(CellUtil.cloneValue(listCell));
                JSONObject jsonObject = JSON.parseObject(Value);
                String source_id = jsonObject.getString("source_id");
                String lucContno = jsonObject.getString("contno");

                if ("1".equals(source_id)) {

                    Result lupResult = getHbaseResult(lucContno, lupTable);
                    if(!lupResult.isEmpty()){
                        for (Cell cell : lupResult.listCells()) {
                            //获取宽表主键
                            String Value1 = Bytes.toString(CellUtil.cloneValue(cell));
                            JSONObject jsonObject1 = JSON.parseObject(Value1);
                            String rowKey = jsonObject1.getString("polno");
                            Put put = new Put(Bytes.toBytes(rowKey));
                            put.addColumn(cf, Bytes.toBytes("fp1_lbappnt_1" + contno), Bytes.toBytes(hb1Value));
                            kuanbiao.put(put);
                        }
                    }
                }
            }
        }
    }
}
