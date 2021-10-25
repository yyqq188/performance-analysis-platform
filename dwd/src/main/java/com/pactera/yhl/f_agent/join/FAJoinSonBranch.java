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
 * @Description F_Agent 机构表 中支
 * @create 2021/10/19 17:47
 */
public class FAJoinSonBranch extends AbstractJoin<T01branchinfo, T02salesinfo_k>{
    //中间表对象
    private AsyncTable<AdvancedScanResultConsumer> aTable;
    private AsyncTable<AdvancedScanResultConsumer> oxtotable;
    //宽表对象
    private AsyncTable<AdvancedScanResultConsumer> kuanbiao;

    @Override
    public void genTableConnection(CompletableFuture<AsyncConnection> conn, Configuration hbaseConfig) throws Exception {
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            aTable = connection.getTable(TableName.valueOf("KL:O_XG_T01BRANCHINFO_BRANCH_ID_PARENT"));
            oxtotable = connection.getTable(TableName.valueOf("KL:O_XG_T02SALESINFO_K_BRANCH_ID"));
            kuanbiao = connection.getTable(TableName.valueOf("KL:KB_F_AGENT"));
        }
    }
//    left join t01branchinfo a3 -- 总公司
//    on a3.branch_id = a2.branch_id_parent and a3.class_id = '1'
    @Override
    public void asyncHandler(T01branchinfo a3) throws Exception {
        //通过关联条件获取a3表信息
        String a3Branch_id = a3.getBranch_id();
        String a3Class_id = a3.getClass_id();
        if ("1".equals(a3Class_id)) {
            //插入宽表信息
            String a3Value = JSON.toJSONString(a3, SerializerFeature.WriteMapNullValue,
                    SerializerFeature.DisableCircularReferenceDetect,
                    SerializerFeature.WriteDateUseDateFormat);
            //通过关联条件获取a2表信息
            Result a2Result = getHbaseResult(a3Branch_id + "", this.aTable);
            if (!a2Result.isEmpty()) {
                for (Cell a2ListCell : a2Result.listCells()) {
//                    left join t01branchinfo a2 -- 分公司
//                    on a2.branch_id = a1.branch_id_parent and a2.class_id = '2'
                    String a2Value = Bytes.toString(CellUtil.cloneValue(a2ListCell));
                    JSONObject a2JsonObject = JSON.parseObject(a2Value);
                    //获取a2主键及关联字段
                    String a2Branch_id = a2JsonObject.getString("branch_id");
                    String a2Class_id = a2JsonObject.getString("class_id");
                    if ("2".equals(a2Class_id)){
                        //通过关联条件获取a1表信息
                        Result a1Result = getHbaseResult(a2Branch_id + "", this.aTable);
                        if (!a1Result.isEmpty()) {
                            for (Cell a1ListCell : a1Result.listCells()) {
                                String a1Value = Bytes.toString(CellUtil.cloneValue(a1ListCell));
                                JSONObject a1JsonObject = JSON.parseObject(a1Value);
//                                LEFT JOIN ${ods_umss}T01BRANCHINFO A1 -- 中支
//                                ON A1.BRANCH_ID = A.BRANCH_ID_PARENT AND A1.CLASS_ID = '3'
                                //获取a1主键及关联字段
                                String a1Branch_id = a1JsonObject.getString("branch_id");
                                String a1Class_id = a1JsonObject.getString("class_id");
                                if ("3".equals(a1Class_id)) {
                                    //通过关联条件获取a表信息
                                    Result aResult = getHbaseResult(a1Branch_id + "", this.aTable);
                                    if (!aResult.isEmpty()){
                                        for (Cell aListCell : aResult.listCells()) {
                                            //获取a主键及关联字段
                                            String aValue = Bytes.toString(CellUtil.cloneValue(aListCell));
                                            JSONObject aJsonObject = JSON.parseObject(aValue);
                                            String aClass_id = aJsonObject.getString("class_id");
                                            String aBranch_id = aJsonObject.getString("branch_id");
                                            if ("4".equals(aClass_id)){
                                                //A.BRANCH_ID = OXTO.BRANCH_ID
                                                //通过关联条件获取OXTO表信息
                                                Result oxtoResult = getHbaseResult(aBranch_id + "", this.oxtotable);
                                                if(!oxtoResult.isEmpty()){
                                                    for (Cell oxtoListCell : oxtoResult.listCells()) {
                                                        String oxtoValue = Bytes.toString(CellUtil.cloneValue(oxtoListCell));
                                                        JSONObject oxtoJsonObject = JSON.parseObject(oxtoValue);
                                                        String rowKey = oxtoJsonObject.getString("sales_id");
                                                        Put put = new Put(Bytes.toBytes(rowKey));
                                                        put.addColumn(cf,
                                                                Bytes.toBytes("fa_t01branchinfo1" + a3Branch_id),
                                                                Bytes.toBytes(a3Value));
                                                        kuanbiao.put(put);
                                                    } }
                                            } } } } } } } } }
        }
    }

}
