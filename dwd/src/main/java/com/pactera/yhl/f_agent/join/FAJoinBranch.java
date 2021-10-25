package com.pactera.yhl.f_agent.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.T01branchinfo;
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
 * @Description F_Agent 机构表
 * @create 2021/10/19 18:52
 */
public class FAJoinBranch extends AbstractJoin<T01branchinfo, T02salesinfo_k>{
    //中间表对象
    private AsyncTable<AdvancedScanResultConsumer> oxtotable;
    //宽表对象
    private AsyncTable<AdvancedScanResultConsumer> kuanbiao;
    @Override
    public void genTableConnection(CompletableFuture<AsyncConnection> conn, Configuration hbaseConfig) throws Exception {
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            oxtotable = connection.getTable(TableName.valueOf("KL:O_XG_T02SALESINFO_K_BRANCH_ID"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_AGENT"));
        }
    }
//    LEFT JOIN T01branchinfo A
//    ON A.BRANCH_ID = OXTO.BRANCH_ID
    @Override
    public void asyncHandler(T01branchinfo a) throws Exception {
        //获取a主键及关联字段
        String branch_id = a.getBranch_id();
        //插入宽表信息
        String aValue = JSON.toJSONString(a, SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.WriteDateUseDateFormat);
        //通过关联条件获取a表信息
        Result oxtoResult = getHbaseResult(branch_id + "", this.oxtotable);
        if (!oxtoResult.isEmpty()) {
            for (Cell oxtoListCell : oxtoResult.listCells()) {
                String oxtoValue = Bytes.toString(CellUtil.cloneValue(oxtoListCell));
                JSONObject oxtoJsonObject = JSON.parseObject(oxtoValue);
                String rowKey = oxtoJsonObject.getString("sales_id");
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(cf,Bytes.toBytes("t01branchinfo" + branch_id),
                        Bytes.toBytes(aValue));
                kuanbiao.put(put);
            }
        }
    }

}
