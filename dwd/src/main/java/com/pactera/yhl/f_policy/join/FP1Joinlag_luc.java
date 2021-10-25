package com.pactera.yhl.f_policy.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.KL_lucont;

import com.pactera.yhl.entity.T01teaminfo;
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
 * @Description 接收 ods.o_lis_LABranchGroup lag , 插入luc表数据, 取 代理人机构名称
 * @create 2021/10/14 13:17
 */
public class FP1Joinlag_luc extends AbstractJoin<T01teaminfo, KL_lucont>{
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
            lucTable = connection.getTable(TableName.valueOf("KL:LUCONT_AGENTGROUP"));
            lupTable = connection.getTable(TableName.valueOf("KL:LUPOL_CONTNO"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_POLICY"));
        }
    }
//    left join   ODS.O_XG_T01TEAMINFO lag --代理人机构名称
//    on lag.team_id = luc.agentgroup
//    primarykey : team_id
    @Override
    public void asyncHandler(T01teaminfo lag) throws Exception {
        //获取lag主键及关联字段
        String team_id = lag.getTeam_id();
        //插入宽表信息
        String lagValue = JSON.toJSONString(lag, SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.WriteDateUseDateFormat);
        //通过关联条件获取luc表信息
        Result lucResult = getHbaseResult(team_id + "", this.lucTable);
        if(!lucResult.isEmpty()){
            for (Cell lucListCell : lucResult.listCells()){
                String lucValue = Bytes.toString(CellUtil.cloneValue(lucListCell));
                JSONObject lucjsonObject = JSON.parseObject(lucValue);
                String contno = lucjsonObject.getString("contno");
                Result lupResult = getHbaseResult(contno + "", this.lupTable);
                if (!lupResult.isEmpty()){
                    for(Cell lupListCell : lupResult.listCells()){
                        String lupValue = Bytes.toString(CellUtil.cloneValue(lupListCell));
                        JSONObject lupjsonObject = JSON.parseObject(lupValue);
                        String rowKey = lupjsonObject.getString("polno");
                        Put put = new Put(Bytes.toBytes(rowKey));
                        put.addColumn(cf,Bytes.toBytes("fp1_t01teaminfo" + team_id),
                                Bytes.toBytes(lagValue));
                        kuanbiao.put(put);
                    }
                }
            }
        }
    }

}
