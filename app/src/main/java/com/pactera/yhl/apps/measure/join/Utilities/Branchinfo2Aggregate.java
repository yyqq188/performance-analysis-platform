package com.pactera.yhl.apps.measure.join.Utilities;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.measure.SunUtils;
import com.pactera.yhl.apps.measure.entity.Measure1;
import com.pactera.yhl.apps.measure.entity.Measure2;
import com.pactera.yhl.apps.measure.join.AbstractInsertKafka;
import com.pactera.yhl.util.Util;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/11/19 18:12
 */
public class Branchinfo2Aggregate extends AbstractInsertKafka<Measure1> {
    //判断是否是 上海 / 浙江 / 宁波 分公司
    private static final List<String> branchList = Arrays.asList("310000", "330000", "920000");

    public static int i = 1;
    public Branchinfo2Aggregate(String topic){
        tableName = "KLMIDAPP:T01BRANCHINFO_BRANCH_ID";//HBase中间表名
        this.topic = topic;
    }
    @Override
    public void handle(Measure1 value, Context context, HTable hTable) throws Exception {
        String branch_id = value.getBranch_id();
        String branch_id_parent = "";
        String branch_name = "";
        String class_id  = "";
        String specialFlag = "";
        if (branchList.contains(branch_id)) {
            specialFlag = "YES";
        }else {
            Result branchResult = Util.getHbaseResultSync(branch_id, hTable);
            JSONObject jsonObject = new JSONObject();
            if (!branchResult.isEmpty()) {
                for (Cell branchListCell : branchResult.listCells()) {
                    jsonObject = JSONObject.parseObject(Bytes.toString(CellUtil.cloneValue(branchListCell)));
                    class_id = jsonObject.getString("class_id");
                    branch_id_parent = jsonObject.getString("branch_id_parent");
                }
            }
            while (!"2".equalsIgnoreCase(class_id)) {
                if(StringUtils.isNotBlank(branch_id_parent)){
                    String parentValue = SunUtils.IsSpecialBranch(branch_id_parent, hTable);
                    jsonObject = JSONObject.parseObject(parentValue);
                    class_id = jsonObject.getString("class_id");
                    branch_id_parent = jsonObject.getString("branch_id_parent");
                }else {
                    break;
                }

            }
            branch_id = jsonObject.getString("branch_id");
            branch_name = jsonObject.getString("branch_name");

            if(branchList.contains(branch_id)){
                specialFlag = "YES";
            }else {
                specialFlag = "NO";
            }

        }
        Measure2 measure2 = new Measure2();
        //基础信息
        measure2.setSlf_prem(value.getSlf_prem());
        measure2.setTeam_prem(value.getTeam_prem());
        measure2.setSales_id(value.getSales_id());
        measure2.setBranch_id(value.getBranch_id());
        measure2.setTeam_id(value.getTeam_id());
        measure2.setRank(value.getRank());
        measure2.setLabor(value.getLabor());
        measure2.setHire_date(value.getHire_date());
        measure2.setSales_name(value.getSales_name());
        measure2.setStat(value.getStat());
        measure2.setWorkarea(value.getWorkarea());

        //分公司信息
        measure2.setProvincecom_code(branch_id);
        measure2.setProvincecom_name(branch_name);

        //部信息
        measure2.setDepartment_agentcode(value.getDepartment_agentcode());
        measure2.setDepartment_leader(value.getDepartment_leader());
        measure2.setDepartment_code(value.getDepartment_code());
        measure2.setDepartment_name(value.getDepartment_name());

        //如果变动则为重新求和值
        measure2.setDepartment_m_fyp(value.getDepartment_m_fyp());
        measure2.setDepartment_q_fyp(value.getDepartment_q_fyp());
        measure2.setDepartment_assessment(value.getDepartment_assessment());
        measure2.setDepartment_activity_m(value.getDepartment_activity_m());
        measure2.setDepartment_activity_q(value.getDepartment_activity_q());

        //区信息
        measure2.setDistrict_agentcode(value.getDistrict_agentcode());
        measure2.setDistrict_leader(value.getDistrict_leader());
        measure2.setDistrict_code(value.getDistrict_code());
        measure2.setDistrict_name(value.getDistrict_name());

        //如果变动则为重新求和值
        measure2.setDistinct_m_fyp(value.getDistinct_m_fyp());
        measure2.setDistinct_q_fyp(value.getDistinct_q_fyp());
        measure2.setDistinct_assessment(value.getDistinct_assessment());
        measure2.setDistinct_activity_m(value.getDistinct_activity_m());
        measure2.setDistinct_activity_q(value.getDistinct_activity_q());

        measure2.setFlag(specialFlag);//是否特殊公司标识
        measure2.setIs_change(value.getIs_change());//是否变动部门标识

        producer.send(new ProducerRecord<>(topic, JSON.toJSONString(measure2)));

        System.out.println(i++);
    }

}
