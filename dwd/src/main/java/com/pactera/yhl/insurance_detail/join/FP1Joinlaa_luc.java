package com.pactera.yhl.insurance_detail.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.KL_lucont;

import com.pactera.yhl.entity.T02salesinfo_k;
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
 * @Description 接收 ODS.O_XG_T02SALESINFO_K laa, 插入luc表数据, 取 代理人姓名
 * @create 2021/10/14 11:29
 */
public class FP1Joinlaa_luc extends AbstractJoin<T02salesinfo_k, KL_lucont>{
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
            lucTable = connection.getTable(TableName.valueOf("KL:LUCONT_AGENTCODE"));
            lupTable = connection.getTable(TableName.valueOf("KL:LUPOL_CONTNO"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_POLICY"));
        }
    }
//    left join ODS.O_XG_T02SALESINFO_K laa --代理人姓名，
//    on laa.SALES_ID = luc.AgentCode
//    primarykey : sales_id
    @Override
    public void asyncHandler(T02salesinfo_k laa) throws Exception {
        //获取laa主键及关联字段
        String sales_id = laa.getSales_id();
        //插入宽表信息
        String laaValue = JSON.toJSONString(laa, SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.WriteDateUseDateFormat);
        //通过关联条件获取luc表信息
        Result lucResult = getHbaseResult(sales_id + "", this.lucTable);
        if(!lucResult.isEmpty()){
            for (Cell lucListCell : lucResult.listCells()){
                String lucValue = Bytes.toString(CellUtil.cloneValue(lucListCell));
                JSONObject lucjsonObject = JSON.parseObject(lucValue);
                String lucContno = lucjsonObject.getString("contno");
                //通过关联条件获取lup表信息
                Result lupResult = getHbaseResult(lucContno + "", this.lupTable);
                if ("1".equals( lucjsonObject.getString("source_id"))){
                    if(!lupResult.isEmpty()){
                        for(Cell lupListCell : lupResult.listCells()){
                            String lupValue = Bytes.toString(CellUtil.cloneValue(lupListCell));
                            JSONObject lupjsonObject = JSON.parseObject(lupValue);
                            String rowKey = lupjsonObject.getString("polno");
                            Put put = new Put(Bytes.toBytes(rowKey));
                            put.addColumn(cf,Bytes.toBytes("fp1_t02salesinfo_k" + sales_id),
                                    Bytes.toBytes(laaValue));
                            kuanbiao.put(put);
                        }
                    }
                }
            }
        }
    }

}
