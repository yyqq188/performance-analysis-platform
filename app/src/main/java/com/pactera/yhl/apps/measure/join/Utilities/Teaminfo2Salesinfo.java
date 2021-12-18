package com.pactera.yhl.apps.measure.join.Utilities;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.measure.entity.Measure1;
import com.pactera.yhl.apps.measure.entity.Sales2Team;
import com.pactera.yhl.apps.measure.join.AbstractInsertKafka;
import com.pactera.yhl.util.Util;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Teaminfo2Salesinfo extends AbstractInsertKafka<Sales2Team> {
    protected static int i = 1;
    public Teaminfo2Salesinfo(String topic){
        tableName = "KLMIDAPP:T02SALESINFO_K_SALES_ID";//HBase中间表名
        this.topic = topic;
    }
    @Override
    public void handle(Sales2Team value, Context context, HTable hTable) throws Exception {
        String sales_id = value.getSales_id();
        String department_agentcode = value.getDepartment_agentcode();
        String district_agentcode = value.getDistrict_agentcode();
        Result result = Util.getHbaseResultSync(sales_id, hTable);
        Measure1 measure = new Measure1();
        if (!result.isEmpty()) {
            for (Cell listCell : result.listCells()) {
                JSONObject jsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                measure.setSlf_prem(value.getSlf_prem());
                measure.setTeam_prem(value.getTeam_prem());
                measure.setSales_id(sales_id);
                measure.setBranch_id(jsonObject.getString("branch_id"));
                measure.setTeam_id(jsonObject.getString("team_id"));
                measure.setRank(jsonObject.getString("rank"));
                measure.setHire_date(jsonObject.getString("probation_date"));
                measure.setSales_name(jsonObject.getString("sales_name"));
                measure.setStat(jsonObject.getString("stat"));
                measure.setWorkarea(jsonObject.getString("workarea"));
            }
        }
        //查询上级部门名称
        if(StringUtils.isNotBlank(department_agentcode)){
            Result departResult = Util.getHbaseResultSync(department_agentcode, hTable);
            if(!departResult.isEmpty()){
                for (Cell listCell : departResult.listCells()) {
                    JSONObject jsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                    measure.setDepartment_leader(jsonObject.getString("sales_name"));
                }
            }
        }else {
            measure.setDepartment_leader("");
        }
        measure.setDepartment_agentcode(department_agentcode);
        measure.setDepartment_code(value.getDepartment_code());
        measure.setDepartment_name(value.getDepartment_name());

        //查询上上级部门名称
        if(StringUtils.isNotBlank(district_agentcode)){
            Result disResult = Util.getHbaseResultSync(district_agentcode, hTable);
            if(!disResult.isEmpty()){
                for (Cell listCell : disResult.listCells()) {
                    JSONObject jsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                    measure.setDistrict_leader(jsonObject.getString("sales_name"));
                }
            }
        }else {
            measure.setDistrict_leader("");
        }

        measure.setDistrict_agentcode(district_agentcode);
        measure.setDistrict_code(value.getDistrict_code());
        measure.setDistrict_name(value.getDistrict_name());

        measure.setIs_change(value.getIs_change());

        producer.send(new ProducerRecord<>(topic, JSON.toJSONString(measure)));

        System.out.println(i++);
    }

}
