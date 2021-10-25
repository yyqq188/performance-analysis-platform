package com.pactera.yhl.f_policy.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.KL_lupol;
import com.pactera.yhl.entity.Ldcode;
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
 * @Description ods.o_lis_ldcode ld1, 插入lm表数据, 取 险种 risktype3
 * @create 2021/10/14 14:55
 */
public class FP1Joinld1_lm_lup extends AbstractJoin<Ldcode, KL_lupol>{
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
            lmTable = connection.getTable(TableName.valueOf("KL:LMRISKAPP_RISKTYPE3"));
            lupTable = connection.getTable(TableName.valueOf("KL:LUPOL_RISKCODE"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_POLICY"));
        }
    }
//    left join ods.o_lis_ldcode ld1  --获取险种1 risktype3
//    on ld1.codetype='risktype3' and lm.RiskType3=ld1.code
//    primarykey : codetype, code
    @Override
    public void asyncHandler(Ldcode ld1) throws Exception {
        //获取ld1主键及关联字段
        String codetype = ld1.getCodetype();
        String code = ld1.getCode();
        if("risktype3".equalsIgnoreCase(codetype)){
            //插入宽表信息
            String ld1Value = JSON.toJSONString(ld1, SerializerFeature.WriteMapNullValue,
                    SerializerFeature.DisableCircularReferenceDetect,
                    SerializerFeature.WriteDateUseDateFormat);
            //通过关联条件获取lm表信息
            Result lmResult = getHbaseResult(code + "", this.lmTable);
            if(!lmResult.isEmpty()){
                for (Cell lmListCell : lmResult.listCells()){
                    String lmValue = Bytes.toString(CellUtil.cloneValue(lmListCell));
                    JSONObject lmjsonObject = JSONObject.parseObject(lmValue);
                    //获取lm主键及关联字段
                    String lmriskcode = lmjsonObject.getString("riskcode");
                    Result lupResult = getHbaseResult(lmriskcode + "", this.lupTable);
                    if(!lupResult.isEmpty()){
                        for (Cell lupListCell : lupResult.listCells()) {
                            String lupValue = Bytes.toString(CellUtil.cloneValue(lupListCell));
                            //获取宽表主键
                            String rowKey = JSON.parseObject(lupValue).getString("polno");
                            Put put = new Put(Bytes.toBytes(rowKey));
                            put.addColumn(cf, Bytes.toBytes("fp1_ldcode1" + codetype + code), Bytes.toBytes(ld1Value));
                            kuanbiao.put(put);
                        }
                    }
                }
            }
        }
    }
}
