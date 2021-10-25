package com.pactera.yhl.f_policy.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.KL_lpedoritem;
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
 * @description:
 * @author: TSY
 * @create: 2021/10/14 0014 上午 11:15
 * @Desc:left   join kl_lpedoritem lp
 *              on lp.edorno=lup.edorno and lup.source_id='1' --限定整单退保 1009 修改
 *              and lp.contno=luc.contno  -- ('CT', 'WT', 'XT', 'NC', 'TT','EA')退保这些类型有描述，其他的没有描述，也要算
 */
public class FP1Lp_LucLup extends AbstractJoin<KL_lpedoritem, KL_lupol>{
    //中间表对象
    private AsyncTable<AdvancedScanResultConsumer> lupTable;
    private AsyncTable<AdvancedScanResultConsumer> lucTable;
    //宽表对象
    private AsyncTable<AdvancedScanResultConsumer> kuanbiao;

    @Override
    public void genTableConnection(CompletableFuture conn, Configuration hbaseConfig) throws Exception{
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            lupTable = connection.getTable(TableName.valueOf("KL:LUPOL_EDORNO"));//lup.edorno
            lucTable = connection.getTable(TableName.valueOf("KL:LUCONT_CONTNO"));//luc.contno
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_POLICY"));
        }
    }

    @Override
    public void asyncHandler(KL_lpedoritem lp) throws Exception {

        //contno + edoracceptno + edorno + edortype + insuredno + polno
        //获取关联字段值
        String edorno = lp.getEdorno();
        String contno = lp.getContno();
        String edoracceptno = lp.getEdoracceptno();
        String edortype = lp.getEdortype();
        String insuredno = lp.getInsuredno();
        String polno = lp.getPolno();

        //将POJO类转换成JSON字符串
        String lpValue = JSON.toJSONString(lp, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);

        Result lupResult = getHbaseResult(edorno, lupTable);
        if (!lupResult.isEmpty()) {
            for (Cell listCell : lupResult.listCells()) {
                String lupValue = Bytes.toString(CellUtil.cloneValue(listCell));
                JSONObject jsonObject = JSON.parseObject(lupValue);
                String lupSource_id = jsonObject.getString("source_id");
                //判断lup.source_id='1'
                if ("1".equals(lupSource_id)) {
                    Result lucResult = getHbaseResult(contno, lucTable);
                    if (!lucResult.isEmpty()) {
                        for (Cell cell : lucResult.listCells()) {
                            //获取宽表主键
                            String rowKey = JSON.parseObject(lupValue).getString("polno");
                            Put put = new Put(Bytes.toBytes(rowKey));
                            put.addColumn(cf, Bytes.toBytes("fp1_kl_lpedoritem" + contno + edoracceptno + edorno + edortype + insuredno + polno), Bytes.toBytes(lpValue));
                            kuanbiao.put(put);
                        }
                    }
                }
            }
        }
    }
}
