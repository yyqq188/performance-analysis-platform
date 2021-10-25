package com.pactera.yhl.f_agent.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.T01teaminfo;
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
 * @Description
 * @create 2021/10/20 10:23
 */
public class FAJoinTeam extends AbstractJoin<T01teaminfo, T02salesinfo_k>{
    //中间表对象
    private AsyncTable<AdvancedScanResultConsumer> oxtotable;
    //宽表对象
    private AsyncTable<AdvancedScanResultConsumer> kuanbiao;

    @Override
    public void genTableConnection(CompletableFuture<AsyncConnection> conn, Configuration hbaseConfig) throws Exception {
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            oxtotable = connection.getTable(TableName.valueOf("KL:O_XG_T02SALESINFO_K_TEAM_ID"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_AGENT"));
        }
    }
    //    WHERE OXTT.TEAM_LVL = '1' and OXTT.TEAM_ID = OXTO.TEAM_ID
    @Override
    public void asyncHandler(T01teaminfo b) throws Exception {
        //获取OXTT2主键及关联字段
        String team_id = b.getTeam_id();
        String team_lvl = b.getTeam_lvl();
        //插入宽表信息
        String bValue = JSON.toJSONString(b, SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.WriteDateUseDateFormat);
        //通过关联条件获取OXTO表信息
        Result oxtoResult = getHbaseResult(team_id + "", this.oxtotable);
        if (!oxtoResult.isEmpty()) {
            for (Cell oxtoListCell : oxtoResult.listCells()) {
                String oxtoValue = Bytes.toString(CellUtil.cloneValue(oxtoListCell));
                JSONObject oxtoJsonObject = JSON.parseObject(oxtoValue);
                String rowKey = oxtoJsonObject.getString("sales_id");
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(cf,Bytes.toBytes("t01teaminfo" + team_id),
                        Bytes.toBytes(bValue));
                kuanbiao.put(put);} }
    }
}
