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
 * @Description F_Agent 团队表 区团队表
 * @create 2021/10/20 9:24
 */
public class FAJoinRTeam extends AbstractJoin<T01teaminfo, T02salesinfo_k>{
    //中间表对象
    private AsyncTable<AdvancedScanResultConsumer> bTable;
    private AsyncTable<AdvancedScanResultConsumer> oxtotable;
    //宽表对象
    private AsyncTable<AdvancedScanResultConsumer> kuanbiao;
    @Override
    public void genTableConnection(CompletableFuture<AsyncConnection> conn, Configuration hbaseConfig) throws Exception {
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            bTable = connection.getTable(TableName.valueOf("KL:O_XG_T01TEAMINFO_TEAM_ID_PARENT"));
            oxtotable = connection.getTable(TableName.valueOf("KL:O_XG_T02SALESINFO_K_TEAM_ID"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_AGENT"));
        }
    }
//    LEFT JOIN ODS.O_XG_T01TEAMINFO OXTT2 --区
//    ON OXTT.TEAM_ID_PARENT = OXTT2.TEAM_ID AND OXTT2.TEAM_LVL = '1'
//    WHERE OXTT.TEAM_LVL = '2' and OXTT.TEAM_ID = OXTO.TEAM_ID
    @Override
    public void asyncHandler(T01teaminfo b2) throws Exception {
        //获取OXTT2主键及关联字段
        String b2Team_id = b2.getTeam_id();
        String b2Team_lvl = b2.getTeam_lvl();
        if ("1".equals(b2Team_lvl)){
            //插入宽表信息
            String b2Value = JSON.toJSONString(b2, SerializerFeature.WriteMapNullValue,
                    SerializerFeature.DisableCircularReferenceDetect,
                    SerializerFeature.WriteDateUseDateFormat);
            //通过关联条件获取OXTT表信息
            Result bResult = getHbaseResult(b2Team_id + "", this.bTable);
            if (!bResult.isEmpty()) {
                for (Cell bListCell : bResult.listCells()) {
                    String bValue = Bytes.toString(CellUtil.cloneValue(bListCell));
                    JSONObject bJsonObject = JSON.parseObject(bValue);
                    String bTeam_lvl = bJsonObject.getString("team_lvl");
                    String bTeam_id = bJsonObject.getString("team_id");
                    if ("2".equals(bTeam_lvl)) {
                        //通过关联条件获取OXTO表信息
                        Result oxtoResult = getHbaseResult(b2Team_id + "", this.oxtotable);
                        if (!oxtoResult.isEmpty()) {
                            for (Cell oxtoListCell : oxtoResult.listCells()) {
                                String oxtoValue = Bytes.toString(CellUtil.cloneValue(oxtoListCell));
                                JSONObject oxtoJsonObject = JSON.parseObject(oxtoValue);
                                String rowKey = oxtoJsonObject.getString("sales_id");
                                Put put = new Put(Bytes.toBytes(rowKey));
                                put.addColumn(cf,Bytes.toBytes("t01teaminfo" + b2Team_id),
                                        Bytes.toBytes(b2Value));
                                kuanbiao.put(put);} } } } } }
    }

}
