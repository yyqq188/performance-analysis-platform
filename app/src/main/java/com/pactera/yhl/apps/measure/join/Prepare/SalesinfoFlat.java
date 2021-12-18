package com.pactera.yhl.apps.measure.join.Prepare;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.entity.source.T02salesinfok;
import com.pactera.yhl.sink.abstr.MyHbaseCli;
import com.pactera.yhl.util.Util;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/11/12 15:57
 */
public class SalesinfoFlat extends RichSinkFunction<T02salesinfok> {
    protected static final String cfString = "f";
    protected static final byte[] cf = Bytes.toBytes(cfString);
    public static Connection connection;
    public static HTable salesTable = null;
    public static HTable teamTable = null;
    public static HTable historyTable = null;
    public static Delete delete = null;

    protected static Calendar calendar = Calendar.getInstance();

    public static int i = 1;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String config_path = params.get("config_path");
        connection = MyHbaseCli.hbaseConnection(config_path);
        //关联人员拉平表
        salesTable = (HTable) connection.getTable(TableName.valueOf("KLMIDAPP:T02SALESINFOK_LEADER_ID"));
        //查询团队表,寻找上级
        teamTable = (HTable) connection.getTable(TableName.valueOf("KLMIDAPP:T01TEAMINFO_TEAM_ID"));
        //历史表
        historyTable = (HTable) connection.getTable(TableName.valueOf("kl_base:application_assessment_persion_result"));
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public void invoke(T02salesinfok value, Context context) throws Exception {
        String sales_id = value.getSales_id();
        String team_id = value.getTeam_id();

        Get teamGet = new Get(Bytes.toBytes(team_id));
        //把同层级的所有人都插入到当前 leader_id 下
        //Todo 需要查询离线数据
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        calendar.setTime(sdf.parse(sdf.format(System.currentTimeMillis())));
        calendar.add(Calendar.DAY_OF_MONTH,-1);
        String rowKey = sdf.format(calendar.getTime()) + sales_id;

        //找上级
        Result parentResult = teamTable.get(teamGet);
        if (!parentResult.isEmpty()) {
            for (Cell parentListCell : parentResult.listCells()) {
                JSONObject parentJsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(parentListCell)));
                //上级号
                String parentLeader_id = parentJsonObject.getString("leader_id");
                //上级团队号
                String team_id_parent = parentJsonObject.getString("team_id_parent");
                if (StringUtils.isNotBlank(parentLeader_id) && StringUtils.isNotBlank(team_id_parent)) {
                    //插入到 上级 下
                    Put put = new Put(Bytes.toBytes(parentLeader_id));
                    put.addColumn(cf, Bytes.toBytes(sales_id), Bytes.toBytes(JSONObject.toJSONString(value)));
                    salesTable.put(put);
                    System.out.println(i++);
                    //判断是否变部门
                    Result oldParResult = Util.getHbaseResultSync(rowKey,historyTable);
                    if (!oldParResult.isEmpty()) {
                        for (Cell oldListCell : oldParResult.listCells()) {
                            //旧
                            String oldLeader_id = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(oldListCell))).getString("department_code");
                            //如果改变团队,则删除旧数
                            if(!parentLeader_id.equalsIgnoreCase(oldLeader_id)){
                                delete = new Delete(Bytes.toBytes(oldLeader_id));
                                delete.addColumn(cf,Bytes.toBytes(sales_id));
                                salesTable.delete(delete);
                            }
                        }
                    }
                    //找上上级
                    Get teamGet1 = new Get(Bytes.toBytes(team_id_parent));
                    Result grandResult = teamTable.get(teamGet1);
                    if (!grandResult.isEmpty()) {
                        for (Cell grandListCell : grandResult.listCells()) {
                            JSONObject grandJsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(grandListCell)));
                            //上上级号
                            String grandLeader_id = grandJsonObject.getString("leader_id");
                            //上上级团队号
//                            String team_id_grand = grandJsonObject.getString("team_id_parent");
//                            && StringUtils.isNotBlank(team_id_grand)
                            if (StringUtils.isNotBlank(grandLeader_id)) {
                                //插入到 上上级 下
                                Put put1 = new Put(Bytes.toBytes(grandLeader_id));
                                put1.addColumn(cf, Bytes.toBytes(sales_id), Bytes.toBytes(JSONObject.toJSONString(value)));
                                salesTable.put(put1);
                                System.out.println(i++);
                                //判断是否变部门
                                Result oldGraResult = Util.getHbaseResultSync(rowKey,historyTable);
                                if (!oldGraResult.isEmpty()) {
                                    for (Cell oldListCell : oldGraResult.listCells()) {
                                        String oldLeader_id = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(oldListCell))).getString("district_code");
                                        //如果改变团队,则删除旧数
                                        if(!grandLeader_id.equalsIgnoreCase(oldLeader_id)){
                                            delete = new Delete(Bytes.toBytes(oldLeader_id));
                                            delete.addColumn(cf,Bytes.toBytes(sales_id));
                                            salesTable.delete(delete);
                                        }
                                    }
                                }


                            }
                        }
                    }


                }
            }
        }

    }

}
