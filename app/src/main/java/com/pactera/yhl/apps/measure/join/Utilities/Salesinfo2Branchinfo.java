package com.pactera.yhl.apps.measure.join.Utilities;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.measure.SunUtils;
import com.pactera.yhl.apps.measure.entity.Measure1;
import com.pactera.yhl.apps.measure.join.AbstractInsertKafkaSun;
import com.pactera.yhl.util.Util;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class Salesinfo2Branchinfo extends AbstractInsertKafkaSun<Measure1> {
    public static List<String> rank1 = Arrays.asList("A01","A02","A03","A04","A05","A06","A07","A08","A09");//专员职级
    public static List<String> rank2 = Arrays.asList("A10","A11","A12","A13","A14");//部经理职级
    public static List<String> rank3 = Arrays.asList("A15","A16","A17","A18","A19");//区域总监职级

    //判断是否是 上海 / 浙江 / 宁波 分公司
    public static List<String> branchList = Arrays.asList("310000", "330000", "920000");

    private static int i = 1;
    public Salesinfo2Branchinfo(String topic) throws IOException {
        tableName = "KLMIDAPP:T02SALESINFOK_LEADER_ID";//HBase中间表名
        tableName1 = "kl_base:application_assessment_persion_result";
        this.topic = topic;

    }
    @Override
    public void handle(Measure1 value, Context context, HTable hTable,HTable hTable1) throws Exception {
        String sales_id = value.getSales_id();
        String rank = value.getRank();
        if(rank1.contains(rank)){
            value.setLabor(0);

            value.setDepartment_m_fyp("");
            value.setDepartment_q_fyp("");
            value.setDepartment_assessment("");
            value.setDepartment_activity_m("");
            value.setDepartment_activity_q("");

            value.setDistinct_m_fyp("");
            value.setDistinct_q_fyp("");
            value.setDistinct_assessment("");
            value.setDistinct_activity_m("");
            value.setDistinct_activity_q("");

        }
        else if(rank2.contains(rank) || rank3.contains(rank)){
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date now = sdf.parse(sdf.format(System.currentTimeMillis()));
            int count = 0;
            Result result = Util.getHbaseResultSync(sales_id, hTable);
            if (!result.isEmpty()) {
                for (Cell listCell : result.listCells()) {
                    JSONObject jsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                    String eachSales_id = jsonObject.getString("sales_id");
                    //入职日期
                    Date probation_date = sdf.parse(jsonObject.getString("probation_date"));
                    Date midSeasonDate = SunUtils.MidSeasonDate(probation_date);
                    if (probation_date.before(midSeasonDate)) {
                        count++;
                    }
                    if(StringUtils.isNotBlank(eachSales_id) && "YES".equalsIgnoreCase(value.getIs_change())){
                        //Todo 查询 APPLICATION_ASSESSMENT_PERSION_RESULT 表,获取历史保费
                        Calendar calendar = Calendar.getInstance();
                        calendar.setTime(now);
                        calendar.add(Calendar.DAY_OF_MONTH,-1);
                        String rowKey = sdf.format(calendar.getTime()) + eachSales_id;
                        //离线结果
                        Result eachResult = Util.getHbaseResultSync(rowKey,hTable1);
                        if (!eachResult.isEmpty()) {
                            double m_fypSum = 0.0;// 月度标准保费和
                            double q_fypSum = 0.0;// 季度标准保费和
                            int activity_m = 0;// 所辖部月度活动人力
                            int activity_q = 0;// 所辖部季度活动人力
                            double standard = 10000.0;
                            for (Cell cell : eachResult.listCells()) {
                                JSONObject eachJsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(cell)));
                                //Todo 获取离线值
                                double eachM_fyp = Double.parseDouble(eachJsonObject.getString("m_fyp"));// 个人月度标准保费
                                double eachQ_fyp = Double.parseDouble(eachJsonObject.getString("q_fyp"));// 个人季度标准保费
                                String provincecom_code = eachJsonObject.getString("provincecom_code");
                                m_fypSum = m_fypSum + eachM_fyp;
                                q_fypSum = q_fypSum + eachQ_fyp;
                                if(branchList.contains(provincecom_code)){
                                    standard = standard * 1.2;
                                }
                                if(eachM_fyp >= standard ){
                                    activity_m++;
                                }
                                if(eachQ_fyp >= standard ){
                                    activity_q++;
                                }
                            }
                            if(rank2.contains(rank)){
                                //判断是否是月初 如果是则全部清零,如果否则加上历史
                                if ("Y".equalsIgnoreCase(SunUtils.IsFirstDayOfSeason(now))) {
                                    value.setDepartment_m_fyp("0");
                                    value.setDepartment_assessment("0");
                                    value.setDepartment_activity_m("0");
                                }else {
                                    value.setDepartment_m_fyp(String.valueOf(m_fypSum));
                                    value.setDepartment_assessment(String.valueOf(count));
                                    value.setDepartment_activity_m(String.valueOf(activity_m));
                                }
                                //判断是否是季初 如果是则全部清零,如果否则加上历史
                                if ("Y".equalsIgnoreCase(SunUtils.IsFirstDayOfSeason(now))) {
                                    value.setDepartment_q_fyp("0");
                                    value.setDepartment_activity_q("0");
                                }else {
                                    value.setDepartment_q_fyp(String.valueOf(q_fypSum));
                                    value.setDepartment_activity_q(String.valueOf(activity_q));
                                }

                            }else {
                                //判断是否是月初 如果是则全部清零,如果否则加上历史
                                if ("Y".equalsIgnoreCase(SunUtils.IsFirstDayOfSeason(now))) {
                                    value.setDistinct_m_fyp("0");
                                    value.setDistinct_assessment("0");
                                    value.setDistinct_activity_m("0");
                                }else {
                                    value.setDistinct_m_fyp(String.valueOf(m_fypSum));
                                    value.setDistinct_assessment(String.valueOf(count));
                                    value.setDistinct_activity_m(String.valueOf(activity_m));
                                }
                                //判断是否是季初 如果是则全部清零,如果否则加上历史
                                if ("Y".equalsIgnoreCase(SunUtils.IsFirstDayOfSeason(now))) {
                                    value.setDistinct_q_fyp("0");
                                    value.setDistinct_activity_q("0");
                                }else {
                                    value.setDistinct_q_fyp(String.valueOf(q_fypSum));
                                    value.setDistinct_activity_q(String.valueOf(activity_q));
                                }
                            }
                        }
                    }

                }
            }
            value.setLabor(count);
        }

        producer.send(new ProducerRecord<>(topic, JSON.toJSONString(value)));

        System.out.println(i++);
    }

}
