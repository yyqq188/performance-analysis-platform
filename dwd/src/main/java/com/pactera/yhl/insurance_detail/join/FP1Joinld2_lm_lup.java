package com.pactera.yhl.insurance_detail.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.Ldcode;
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
 * @Description ods.o_lis_ldcode ld2, 插入lm表数据, 取 险种 risktype4
 * @create 2021/10/14 14:07
 */
public class FP1Joinld2_lm_lup extends AbstractJoin<Ldcode, Lmriskapp>{
    //中间表对象
    private AsyncTable<AdvancedScanResultConsumer> lmTable;
    private AsyncTable<AdvancedScanResultConsumer> lupTable;
    //宽表对象
    private AsyncTable<AdvancedScanResultConsumer> kuanbiao;
    @Override
    public void genTableConnection(CompletableFuture<AsyncConnection> conn, Configuration hbaseConfig) throws Exception {
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            lmTable = connection.getTable(TableName.valueOf("KL:LMRISKAPP_RISKTYPE4"));
            lupTable = connection.getTable(TableName.valueOf("KL:LUPOL_RISKCODE"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_POLICY"));
        }
    }
//    left join ods.o_lis_ldcode ld2 --获取险种2r isktype4
//    on ld2.codetype='risktype4' and lm.RiskType4=ld2.code
//    primarykey : codetype, code
    @Override
    public void asyncHandler(Ldcode ld2) throws Exception {
        //获取lag主键及关联字段
        String codetype = ld2.getCodetype();
        String code = ld2.getCode();
        if("risktype4".equalsIgnoreCase(codetype)){
            //插入宽表信息
            String ld2Value = JSON.toJSONString(ld2, SerializerFeature.WriteMapNullValue,
                    SerializerFeature.DisableCircularReferenceDetect,
                    SerializerFeature.WriteDateUseDateFormat);
            //通过关联条件获取lm表信息
            Result lmResult = getHbaseResult(code + "", this.lmTable);
            if(!lmResult.isEmpty()){
                for (Cell lmListCell : lmResult.listCells()){
                    String lmValue = Bytes.toString(CellUtil.cloneValue(lmListCell));
                    JSONObject lmjsonObject = JSONObject.parseObject(lmValue);
                    String lmriskcode = lmjsonObject.getString("riskcode");
                    Result lupResult = getHbaseResult(lmriskcode + "", this.lupTable);
                    if (!lupResult.isEmpty()){
                        for (Cell lupListCell : lupResult.listCells()) {
                            String lupValue = Bytes.toString(CellUtil.cloneValue(lupListCell));
                            //获取宽表主键
                            String rowKey = JSON.parseObject(lupValue).getString("polno");
                            Put put = new Put(Bytes.toBytes(rowKey));
                            put.addColumn(cf, Bytes.toBytes("fp1_ldcode2" + codetype + code), Bytes.toBytes(ld2Value));
                            kuanbiao.put(put);
                        }
                    }
                }
            }
        }
    }

}
