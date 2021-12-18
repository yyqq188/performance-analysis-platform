package com.pactera.yhl.apps.measure.join.Utilities;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.measure.entity.Lpol2salesinfo;
import com.pactera.yhl.apps.measure.entity.Sales2Team;
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
import java.util.List;

/**
 * @author Sun Haitian
 * @Description 进入计算
 * @create 2021/11/16 17:17
 */
public class Salesinfo2Teaminfo extends AbstractInsertKafkaSun<Lpol2salesinfo> {
    public static List<String> rank1 = Arrays.asList("A01","A02","A03","A04","A05","A06","A07","A08","A09");//专员职级
    public static List<String> rank2 = Arrays.asList("A10","A11","A12","A13","A14");//部经理职级
    public static List<String> rank3 = Arrays.asList("A15","A16","A17","A18","A19");//区域总监职级

    public static int i = 1;

    public Salesinfo2Teaminfo(String topic) throws IOException {
        tableName = "KLMIDAPP:T01TEAMINFO_TEAM_ID";//HBase中间表名
        tableName1 = "kl_base:application_assessment_persion_result";
        this.topic = topic;
    }
    @Override
    public void handle(Lpol2salesinfo value, Context context, HTable hTable,HTable hTable1) throws Exception {
        double prem = value.getPrem();
        String rank = value.getRank();
        String sales_id = value.getSales_id();
        //本级团队号
        String team_id = value.getTeam_id();//440208002002
        Sales2Team samSales2Team = new Sales2Team();

        //Todo 查询 APPLICATION_ASSESSMENT_PERSION_RESULT 表,判断是否发生变动
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(sdf.parse(sdf.format(System.currentTimeMillis())));
        calendar.add(Calendar.DAY_OF_MONTH,-1);
        String rowKey = sdf.format(calendar.getTime()) + sales_id;

        String department_agentcode = "";//旧上级领导号
        String department_code = "";//旧上级团队号
        String district_agentcode = "";//旧上上级领导号
        String district_code = "";//旧上上级团队号

        Result historyResult = Util.getHbaseResultSync(rowKey, hTable1);
        if (!historyResult.isEmpty()) {
            for (Cell listCell : historyResult.listCells()) {
                JSONObject jsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                department_agentcode = jsonObject.getString("department_agentcode");
                department_code = jsonObject.getString("department_code");
                district_agentcode = jsonObject.getString("district_agentcode");
                district_code = jsonObject.getString("district_code");
            }
        }

        Result parentResult = Util.getHbaseResultSync(team_id, hTable);
        if (!parentResult.isEmpty()) {
            //上级对象
            Sales2Team parentSales2Team = new Sales2Team();
            for (Cell parentListCell : parentResult.listCells()) {
                JSONObject parentJsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(parentListCell)));
                //上级团队号
                String team_id_parent = parentJsonObject.getString("team_id_parent");//440208002000
                //上级领导
                String parentLeader_id = parentJsonObject.getString("leader_id");//440208000005
                //本级团队名
                String team_name = parentJsonObject.getString("team_name");//黄俊霖总监第二营业部
                if(StringUtils.isNotBlank(parentLeader_id)){
                    //必输出数据
                    if(rank1.contains(rank)){
                        //部信息
                        samSales2Team.setDepartment_agentcode(parentLeader_id);
                        samSales2Team.setDepartment_code(team_id);
                        samSales2Team.setDepartment_name(team_name);

                        //上级对象
                        parentSales2Team.setSlf_prem(0.0);
                        parentSales2Team.setTeam_prem(prem);
                        parentSales2Team.setSales_id(parentLeader_id);
                        //部信息
                        parentSales2Team.setDepartment_agentcode(parentLeader_id);
                        parentSales2Team.setDepartment_code(team_id);
                        parentSales2Team.setDepartment_name(team_name);

                    }else if(rank2.contains(rank)){
                        samSales2Team.setDistrict_agentcode(parentLeader_id);
                        samSales2Team.setDistrict_code(team_id_parent);
                        samSales2Team.setDistrict_name(team_name);

                        //上级对象
                        parentSales2Team.setSlf_prem(0.0);
                        parentSales2Team.setTeam_prem(prem);
                        parentSales2Team.setSales_id(parentLeader_id);
                        //部信息
                        parentSales2Team.setDepartment_agentcode("");
                        parentSales2Team.setDepartment_code("");
                        parentSales2Team.setDepartment_name("");
                        //区信息
                        parentSales2Team.setDistrict_agentcode(parentLeader_id);
                        parentSales2Team.setDistrict_code(team_id_parent);
                        parentSales2Team.setDistrict_name(team_name);
                    }else if(rank3.contains(rank)){
                        //区信息
                        samSales2Team.setDistrict_agentcode(sales_id);
                        samSales2Team.setDistrict_code(team_id);
                        samSales2Team.setDistrict_name(team_name);
                    }

                    //是否输出数据
                    //判断 是否 发生变动,如果无变动则不发出,发生变动则发出
                    if(StringUtils.isNotBlank(department_agentcode) && !parentLeader_id.equalsIgnoreCase(department_agentcode)){
                        //旧上级对象
                        Sales2Team oldDepart = new Sales2Team();
                        Result depResult = Util.getHbaseResultSync(department_code, hTable);
                        if (!depResult.isEmpty()) {
                            for (Cell listCell : depResult.listCells()) {
                                JSONObject depJsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                                String depTeam_id_parent = depJsonObject.getString("team_id_parent");
                                oldDepart.setSlf_prem(0.0);
                                oldDepart.setTeam_prem(0.0);
                                oldDepart.setSales_id(department_agentcode);
                                //部信息
                                oldDepart.setDepartment_agentcode(depJsonObject.getString("leader_id"));
                                oldDepart.setDepartment_code(depJsonObject.getString("team_id"));
                                oldDepart.setDepartment_name(depJsonObject.getString("team_name"));
                                if(StringUtils.isNotBlank(depTeam_id_parent)){
                                    Result disResult = Util.getHbaseResultSync(depTeam_id_parent, hTable);
                                    if (!disResult.isEmpty()) {
                                        for (Cell cell : disResult.listCells()) {
                                            JSONObject disJsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(cell)));
                                            //区信息
                                            oldDepart.setDistrict_agentcode(disJsonObject.getString("leader_id"));
                                            oldDepart.setDistrict_code(disJsonObject.getString("team_id"));
                                            oldDepart.setDistrict_name(disJsonObject.getString("team_name"));
                                        }
                                    }
                                }
                                oldDepart.setIs_change("YES");
                            }
                        }

                        //发送旧上级数据
                        producer.send(new ProducerRecord<>(topic, JSON.toJSONString(oldDepart)));
                        System.out.println(i++);

                        parentSales2Team.setIs_change("YES");//是否发生变动标识
                    }else {
                        parentSales2Team.setIs_change("NO");//是否发生变动标识
                    }
                }

                if(StringUtils.isNotBlank(team_id_parent)){
                    Result grandResult = Util.getHbaseResultSync(team_id_parent, hTable);
                    if (!grandResult.isEmpty()) {
                        //上上级对象
                        Sales2Team grandSales2Team = new Sales2Team();
                        for (Cell grandListCell : grandResult.listCells()) {
                            JSONObject grandJsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(grandListCell)));
                            //上上级团队号
//                            String team_id_grand = grandJsonObject.getString("team_id_parent");
                            //上上级领导
                            String grandLeader_id = grandJsonObject.getString("leader_id");
                            //上上级团队名
                            String grandTeam_name = grandJsonObject.getString("team_name");
                            if(StringUtils.isNotBlank(grandLeader_id)){
                                if(rank1.contains(rank)){
                                    //区信息
                                    samSales2Team.setDistrict_agentcode(grandLeader_id);
                                    samSales2Team.setDistrict_code(team_id_parent);
                                    samSales2Team.setDistrict_name(grandTeam_name);

                                    //区信息
                                    parentSales2Team.setDistrict_agentcode(grandLeader_id);
                                    parentSales2Team.setDistrict_code(team_id_parent);
                                    parentSales2Team.setDistrict_name(grandTeam_name);

                                    //区信息
                                    grandSales2Team.setDistrict_agentcode(grandLeader_id);
                                    grandSales2Team.setDistrict_code(team_id_parent);
                                    grandSales2Team.setDistrict_name(grandTeam_name);
                                }
                                if(StringUtils.isNotBlank(district_agentcode) && !grandLeader_id.equalsIgnoreCase(district_agentcode)){
                                    //旧上上级对象
                                    Sales2Team oldDis = new Sales2Team();
                                    Result disResult = Util.getHbaseResultSync(district_code, hTable);
                                    if (!disResult.isEmpty()) {
                                        for (Cell listCell : disResult.listCells()) {
                                            JSONObject disJsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                                            oldDis.setSlf_prem(0.0);
                                            oldDis.setTeam_prem(0.0);
                                            oldDis.setSales_id(district_agentcode);
                                            //部信息
                                            oldDis.setDepartment_agentcode("");
                                            oldDis.setDepartment_code("");
                                            oldDis.setDepartment_name("");
                                            //区信息
                                            oldDis.setDistrict_agentcode(disJsonObject.getString("leader_id"));
                                            oldDis.setDistrict_code(district_code);
                                            oldDis.setDistrict_name(disJsonObject.getString("team_name"));
                                            oldDis.setIs_change("YES");
                                        }
                                    }
                                    //发送旧上上级数据
                                    producer.send(new ProducerRecord<>(topic, JSON.toJSONString(oldDis)));
                                    System.out.println(i++);

                                    grandSales2Team.setIs_change("YES");//是否发生变动标识
                                }else {
                                    grandSales2Team.setIs_change("NO");//是否发生变动标识
                                }
                            }
                            grandSales2Team.setSlf_prem(0.0);
                            grandSales2Team.setTeam_prem(prem);
                            grandSales2Team.setSales_id(grandLeader_id);
                            //部信息
                            grandSales2Team.setDepartment_agentcode("");
                            grandSales2Team.setDepartment_code("");
                            grandSales2Team.setDepartment_name("");
                        }
                        if(StringUtils.isNotBlank(grandSales2Team.getSales_id())){
                            //发送上上级数据
                            producer.send(new ProducerRecord<>(topic, JSON.toJSONString(grandSales2Team)));

                            System.out.println(i++);
                        }
                    }
                }

            }
            if(StringUtils.isNotBlank(parentSales2Team.getSales_id())){
                //发送上级数据
                producer.send(new ProducerRecord<>(topic, JSON.toJSONString(parentSales2Team)));

                System.out.println(i++);
            }
        }
        samSales2Team.setSales_id(sales_id);
        samSales2Team.setSlf_prem(prem);
        samSales2Team.setTeam_prem(0.0);
        samSales2Team.setIs_change("NO");//是否发生变动标识
        if (rank3.contains(rank)) {
            //部信息
            samSales2Team.setDepartment_agentcode("");
            samSales2Team.setDepartment_code("");
            samSales2Team.setDepartment_name("");
        }
        //发送本级数据
        producer.send(new ProducerRecord<>(topic, JSON.toJSONString(samSales2Team)));

        System.out.println(i++);
    }

}
