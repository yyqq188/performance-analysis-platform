package com.pactera.yhl.apps.construction.statejob;

import com.google.common.collect.Sets;
import com.pactera.yhl.apps.construction.entity.ConstructionPOJO;
import com.pactera.yhl.apps.construction.entity.PremiumsPOJO;
import com.pactera.yhl.apps.construction.util.DateUDF;
import com.pactera.yhl.apps.construction.util.StateStorage;
import com.pactera.yhl.apps.construction.util.Utils;
import com.pactera.yhl.sink.abstr.MyHbaseCli;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

import static com.pactera.yhl.util.Util.getHbaseValue;


/**
 * @author: TSY
 * @create: 2021/11/15 0015 下午 18:10
 * @description:  中间状态计算层(分公司)
 */
public class MergeCountStateMap extends RichMapFunction<Tuple2<String, PremiumsPOJO>, ConstructionPOJO> {

    //日入职
    MapState<String, Integer> memberCountState;//日入职专员
    MapState<String, Integer> managerCountState;//日入职经理
    MapState<String, Integer> directorCountState;//日入职总监
    //累计入职
    MapState<String, Integer> memberCumulativeState;//累计入职专员
    MapState<String, Integer> managerCumulativeState;//累计入职经理
    MapState<String, Integer> directorCumulativeState;//累计入职总监
    //月入职，季度入职，累计总人力
    MapState<String, Integer> totalMonthCumulativeState;//月入职
    MapState<String, Integer> totalQuarterCumulativeState;//季度入职
    MapState<String, Integer> totalCumulativeState;//累计总人力

    //日期缴活动人力
    MapState<String, Double> sumStateActivity;
    MapState<String, Integer> countStateActivity;
    final static double activitySumStateMapThreshold = 10000.00;
    MapState<String, Double> sumStateMonthActivity;
    MapState<String, Integer> countStateMonthActivity;

    //日入职出单人力
    MapState<String, Double> sumStateIssue;
    MapState<String, Integer> countStateIssue;
    final static double issueSumStateMapThreshold = 0.00;
    MapState<String, Double> sumStateMonthIssue;
    MapState<String, Integer> countStateMonthIssue;

    //日合格人力
    MapState<String, Double> sumStateQualified;
    MapState<String, Integer> countStateQualified;
    final static double qualifiedSumStateMapThreshold = 30000.00;
    MapState<String, Double> sumStateMonthQualified;
    MapState<String, Integer> countStateMonthQualified;

    //当月累计期缴保费
    MapState<String, Double> sumStatePremium;

    //当日离职月期缴活动人力
    MapState<String, Integer> quitState;
    //当日离职月合格人力
    MapState<String, Integer> qualifiedState;

    final static String manpowerTable = "kl_base:application_manpower_result";//离线人力建设数据
    final static String generalTable = "kl_base:application_general_result";//离线机构月期缴保费
    final static String manpower_tmp1Table = "kl_base:application_manpower_result_tmp1";//离线人员期缴保费
    final static String manpower_persionTable = "kl_base:application_assessment_persion_result";//获取分公司判断人员是否变动
    private ConstructionPOJO cp = new ConstructionPOJO();
    private Connection connection = null;
    private HTable hTableManpowerTable =null;
    private HTable hTableManpowerGeneralTable =null;
    private HTable hTableManpower_Tmp1Table =null;
    private HTable hTableManpower_persionTable =null;

    @Override
    public void open(Configuration parameters) throws IOException {

        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String config_path = params.get("config_path");
        connection = MyHbaseCli.hbaseConnection(config_path);
        try{
            //离线人力建设数据
            hTableManpowerTable = (HTable) connection.getTable(TableName.valueOf(manpowerTable));
            //离线机构月期缴保费
            hTableManpowerGeneralTable = (HTable) connection.getTable(TableName.valueOf(generalTable));
            //离线人员期缴保费
            hTableManpower_Tmp1Table = (HTable) connection.getTable(TableName.valueOf(manpower_tmp1Table));
            //获取分公司判断人员是否变动
            hTableManpower_persionTable = (HTable) connection.getTable(TableName.valueOf(manpower_persionTable));
        }catch (Exception e){
            e.printStackTrace();
        }

        //日入职
        memberCountState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "memberCountState", String.class, Integer.class
        ));

        managerCountState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "managerCountState", String.class, Integer.class
        ));

        directorCountState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "directorCountState", String.class, Integer.class
        ));

        //累计入职
        memberCumulativeState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "memberCumulativeState", String.class, Integer.class
        ));

        managerCumulativeState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "managerCumulativeState", String.class, Integer.class
        ));

        directorCumulativeState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "directorCumulativeState", String.class, Integer.class
        ));

        //日期缴活动人力，日入职出单人力，日合格人力
        sumStateActivity = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>(
                "sumStateActivity", String.class, Double.class
        ));

        countStateActivity = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "countStateActivity", String.class, Integer.class
        ));

        sumStateIssue = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>(
                "sumStateIssue", String.class, Double.class
        ));

        countStateIssue = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "countStateIssue", String.class, Integer.class
        ));

        sumStateQualified = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>(
                "sumStateQualified", String.class, Double.class
        ));
        countStateQualified = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "countStateQualified", String.class, Integer.class
        ));

        //月期缴活动人力，月入职出单人力，月合格人力
        sumStateMonthActivity = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>(
                "sumStateMonthActivity", String.class, Double.class
        ));
        countStateMonthActivity = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "countStateMonthActivity", String.class, Integer.class
        ));

        sumStateMonthIssue = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>(
                "sumStateMonthIssue", String.class, Double.class
        ));

        countStateMonthIssue = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
            "countStateMonthIssue", String.class, Integer.class
        ));

        sumStateMonthQualified = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>(
                "sumStateMonthQualified", String.class, Double.class
        ));

        countStateMonthQualified = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
            "countStateMonthQualified", String.class, Integer.class
        ));

        //月，季度，总累计
        totalMonthCumulativeState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "totalMonthCumulativeState", String.class, Integer.class
        ));

        totalQuarterCumulativeState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "totalQuarterCumulativeState", String.class, Integer.class
        ));

        totalCumulativeState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "totalCumulativeState", String.class, Integer.class
        ));

        //期缴总保费
        sumStatePremium = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>(
                "sumStatePremium", String.class, Double.class
        ));

        //当日离职月期缴活动人力
        quitState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "quitState", String.class, Integer.class
        ));

        //当日离职月合格人力
        qualifiedState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "qualifiedState", String.class, Integer.class
        ));
    }

    public ConstructionPOJO map(Tuple2<String, PremiumsPOJO> value) throws Exception {

        String firstDayOfMonth = DateUDF.getFirstDayOfMonth();
        String endDayOfMonth = DateUDF.getEndDayOfMonth();
        String currentDay = DateUDF.getCurrentDay();
        String beforeDay = DateUDF.getbeforeDay();

        String key_id = value.f0;
        System.out.println("key_id"+key_id);
        String rank = value.f1.getRank();
        String branch_name = value.f1.getBranch_name();
        String stat = value.f1.getStat();
        System.out.println(key_id + "   " + rank);
        String sales_id = currentDay +"#"+value.f1.getSales_id();
        String probation_date = value.f1.getProbation_date();//入职日期
        String signdate = value.f1.getSigndate();//签单日期
        Double prem = Double.valueOf(value.f1.getPrem());
        String mark = value.f1.getMark();

        System.out.println("sales_id"+sales_id);


        //用于查询离线数据的key_id，用昨天日期拼上今天的机构代码
        String offLineKey_id = beforeDay +"#"+ key_id.split("#")[1];

        System.out.println("offLineKey_id"+offLineKey_id);

        //获取离线累计值
        if(totalMonthCumulativeState.get(key_id) == null) {
            //月入职
            Integer new_manpower_monthManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, offLineKey_id, "f", offLineKey_id, "new_manpower_month"));
            System.out.println("月入职:" + new_manpower_monthManpower);
            totalMonthCumulativeState.put(key_id, new_manpower_monthManpower);
        }
        //季度入职
        if(totalQuarterCumulativeState.get(key_id) == null) {
            Integer new_manpower_quarterManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, offLineKey_id, "f", offLineKey_id, "new_manpower_quarter"));
            System.out.println("季度入职:" + new_manpower_quarterManpower);
            totalQuarterCumulativeState.put(key_id, new_manpower_quarterManpower);
        }
        if(memberCumulativeState.get(key_id) == null) {
            //累计入职专员
            Integer total_memberManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, offLineKey_id, "f", offLineKey_id, "total_member"));
            System.out.println("累计入职专员:" + total_memberManpower);
            memberCumulativeState.put(key_id, total_memberManpower);
        }

        //累计入职经理
        if(managerCumulativeState.get(key_id) == null) {
            Integer total_managerManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, offLineKey_id, "f", offLineKey_id, "total_manager"));
            System.out.println("累计入职经理:" + total_managerManpower);
            managerCumulativeState.put(key_id, total_managerManpower);
        }
            //累计入职总监
        if(directorCumulativeState.get(key_id) == null) {
            Integer total_directorManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, offLineKey_id, "f", offLineKey_id, "total_director"));
            System.out.println("累计入职总监:" + total_directorManpower);
            directorCumulativeState.put(key_id, total_directorManpower);
        }
        //累计总人力
        if(totalCumulativeState.get(key_id) == null) {
            Integer total_manpowerManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, offLineKey_id, "f", offLineKey_id, "total_manpower"));
            System.out.println("累计总人力:" + total_manpowerManpower);
            totalCumulativeState.put(key_id, total_manpowerManpower);
        }
        //月期缴活动人力
        if(countStateMonthActivity.get(key_id) == null) {
            if("01".equals(currentDay.substring(8, 10))) {
                countStateMonthActivity.put(key_id, 0);
            }else {
                Integer activity_manpower_monthManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, offLineKey_id, "f", offLineKey_id, "activity_manpower_month"));
                System.out.println("月期缴活动人力:" + activity_manpower_monthManpower);
                countStateMonthActivity.put(key_id, activity_manpower_monthManpower);
            }
        }
        //月入职出单人力
        if(countStateMonthIssue.get(key_id) == null) {
            if("01".equals(currentDay.substring(8, 10))) {
                countStateMonthIssue.put(key_id, 0);
            }else {
                Integer enter_order_monthManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, offLineKey_id, "f", offLineKey_id, "enter_order_month"));
                System.out.println("月入职出单人力:" + enter_order_monthManpower);
                countStateMonthIssue.put(key_id, enter_order_monthManpower);
            }
        }
        //月合格人力
        if(countStateMonthQualified.get(key_id) == null) {
            if("01".equals(currentDay.substring(8, 10))) {
                countStateMonthQualified.put(key_id, 0);
            }else {
                Integer eligible_manpower_monthManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, offLineKey_id, "f", offLineKey_id, "eligible_manpower_month"));
                System.out.println("月合格人力:" + eligible_manpower_monthManpower);
                countStateMonthQualified.put(key_id, eligible_manpower_monthManpower);
            }
        }


        //判断人员变动
        //昨天分公司代码
        String provincecom_code = Utils.stringIsNull(getHbaseValue(hTableManpower_persionTable, beforeDay + sales_id.split("#")[1], "f", beforeDay + sales_id.split("#")[1], "provincecom_code"));
        System.out.println("变更后的机构代码provincecom_code : "+provincecom_code);
//        System.out.println(key_id.split("#")[1].compareTo(provincecom_code) != 0);
        //发生人员变动
        if(provincecom_code.length() != 0 && key_id.split("#")[1].compareTo(provincecom_code) != 0){
            String oldKey_id = beforeDay + "#" + provincecom_code;
            String oldKey_IdCurrent = currentDay + "#" + provincecom_code;
            String sales_Id = beforeDay + "#" + sales_id.split("#")[1];
            //旧机构累计总人力减1
            if(totalCumulativeState.get(oldKey_IdCurrent) == null) {
                Integer total_manpowerManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, oldKey_id, "f", oldKey_id, "total_manpower"));
                totalCumulativeState.put(oldKey_IdCurrent, total_manpowerManpower - 1);
            }else {
                totalCumulativeState.put(oldKey_IdCurrent, totalCumulativeState.get(oldKey_IdCurrent) -1);
            }
            //新机构累计总人力加1
            totalCumulativeState.put(key_id, totalCumulativeState.get(key_id) + 1);

            //职级
            String agent_grade = Utils.stringIsNull(getHbaseValue(hTableManpower_persionTable, sales_Id, "f", sales_Id, "agent_grade"));

            //累计专员人数
            if (Sets.newHashSet("A01", "A02", "A03", "A04", "A05", "A06", "A07", "A08", "A09").contains(Utils.stringIsNull(agent_grade))) {
                //存在旧分公司代码
                if(memberCumulativeState.get(oldKey_IdCurrent) != null){
                    memberCumulativeState.put(oldKey_IdCurrent, memberCumulativeState.get(oldKey_IdCurrent) - 1);
                }else {
                    Integer total_memberManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, oldKey_id, "f", oldKey_id,"total_member"));
                    memberCumulativeState.put(oldKey_IdCurrent, total_memberManpower - 1);
                }
                //新机构加1
                memberCumulativeState.put(key_id, memberCumulativeState.get(key_id) + 1);
            //累计入职经理
            }else if (Sets.newHashSet("A10", "A11", "A12", "A13", "A14").contains(Utils.stringIsNull(agent_grade))) {
                //存在旧分公司代码
                if(managerCumulativeState.get(oldKey_IdCurrent) != null){
                    managerCumulativeState.put(oldKey_IdCurrent, managerCumulativeState.get(oldKey_IdCurrent) - 1);
                }else {
                    Integer total_memberManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, oldKey_id, "f", oldKey_id,"total_manager"));
                    managerCumulativeState.put(oldKey_IdCurrent, total_memberManpower - 1);
                }
                //新机构加1
                managerCumulativeState.put(key_id, managerCumulativeState.get(key_id) + 1);
                //累计入职总监
            }else if(Sets.newHashSet("A15", "A16", "A17", "A18", "A19").contains(Utils.stringIsNull(agent_grade))){
                //存在旧分公司代码
                if(directorCumulativeState.get(oldKey_IdCurrent) != null){
                    directorCumulativeState.put(oldKey_IdCurrent, directorCumulativeState.get(oldKey_IdCurrent) - 1);
                }else {
                    Integer total_memberManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, oldKey_id, "f", oldKey_id,"total_director"));
                    directorCumulativeState.put(oldKey_IdCurrent, total_memberManpower - 1);
                }
                //新机构加1
                directorCumulativeState.put(key_id, Utils.int2IntIsNull(directorCumulativeState.get(key_id)) + 1);
            }

            //人力更改
            //日期交活动人力
            if(sumStateActivity.get(sales_id) != null){
                Double sumActivity = sumStateActivity.get(oldKey_IdCurrent);
                if(sumActivity >= activitySumStateMapThreshold){
                    countStateActivity.put(oldKey_IdCurrent, countStateActivity.get(oldKey_IdCurrent) - 1);
                    countStateActivity.put(key_id, Utils.int2IntIsNull(countStateActivity.get(key_id)) + 1);
                }
            }

            //日入职出单人力
            if(sumStateIssue.get(sales_id) != null){
                Double sumActivity = sumStateIssue.get(oldKey_IdCurrent);
                if(sumActivity >= issueSumStateMapThreshold){
                    countStateIssue.put(oldKey_IdCurrent, countStateIssue.get(oldKey_IdCurrent) - 1);
                    countStateIssue.put(key_id, Utils.int2IntIsNull(countStateIssue.get(key_id)) + 1);
                }
            }

            //日合格人力
            if(sumStateQualified.get(sales_id) != null){
                Double sumActivity = sumStateQualified.get(oldKey_IdCurrent);
                if(sumActivity >= qualifiedSumStateMapThreshold){
                    countStateQualified.put(oldKey_IdCurrent, countStateQualified.get(oldKey_IdCurrent) - 1);
                    countStateQualified.put(key_id, Utils.int2IntIsNull(countStateQualified.get(key_id)) + 1);
                }
            }

            //月期交活动人力
            if(!"01".equals(currentDay.substring(8, 10))) {
                if (sumStateMonthActivity.get(sales_id) != null) {
                    Double sumActivity = sumStateMonthActivity.get(sales_id);
                    if (sumActivity >= activitySumStateMapThreshold) {
                        if (countStateMonthActivity.get(oldKey_IdCurrent) == null) {
                            Integer activity_manpower_monthManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, offLineKey_id, "f", offLineKey_id, "activity_manpower_month"));
                            countStateMonthActivity.put(oldKey_IdCurrent, activity_manpower_monthManpower - 1);
                        } else {
                            countStateMonthActivity.put(oldKey_IdCurrent, countStateMonthActivity.get(oldKey_IdCurrent) - 1);
                        }
                        countStateMonthActivity.put(key_id, Utils.int2IntIsNull(countStateMonthActivity.get(key_id)) + 1);
                    }
                } else {
                    Double sumActivity = Utils.str2DouIsNull(getHbaseValue(hTableManpower_Tmp1Table, sales_id.split("#")[1], "f", sales_id.split("#")[1], "m_qj_manpower_prem"));
                    sumStateMonthActivity.put(sales_id, sumActivity);
                    if (sumActivity >= activitySumStateMapThreshold) {
                        if (countStateMonthActivity.get(oldKey_IdCurrent) == null) {
                            Integer activity_manpower_monthManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, offLineKey_id, "f", offLineKey_id, "activity_manpower_month"));
                            countStateMonthActivity.put(oldKey_IdCurrent, activity_manpower_monthManpower - 1);
                        } else {
                            countStateMonthActivity.put(oldKey_IdCurrent, countStateMonthActivity.get(oldKey_IdCurrent) - 1);
                        }
                        countStateMonthActivity.put(key_id, Utils.int2IntIsNull(countStateMonthActivity.get(key_id)) + 1);
                    }
                }
            }else {
                if(sumStateMonthActivity.get(sales_id) != null){
                    Double sumActivity = sumStateMonthActivity.get(sales_id);
                    if(sumActivity >= activitySumStateMapThreshold){
                        countStateMonthActivity.put(oldKey_IdCurrent, countStateMonthActivity.get(oldKey_IdCurrent) - 1);
                        countStateMonthActivity.put(key_id, countStateMonthActivity.get(key_id) + 1);
                    }
                }
            }

            //月入职出单人力
            if(!"01".equals(currentDay.substring(8, 10))) {
                if (sumStateMonthIssue.get(sales_id) != null) {
                    Double sumActivity = sumStateMonthIssue.get(sales_id);
                    if (sumActivity >= issueSumStateMapThreshold) {
                        if (countStateMonthIssue.get(oldKey_IdCurrent) == null) {
                            Integer enter_order_monthManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, offLineKey_id, "f", offLineKey_id, "enter_order_month"));
                            countStateMonthIssue.put(oldKey_IdCurrent, enter_order_monthManpower - 1);
                        } else {
                            countStateMonthIssue.put(oldKey_IdCurrent, countStateMonthIssue.get(oldKey_IdCurrent) - 1);
                        }
                        countStateMonthIssue.put(key_id, Utils.int2IntIsNull(countStateMonthIssue.get(key_id)) + 1);
                    }
                } else {
                    Double sumActivity = Utils.str2DouIsNull(getHbaseValue(hTableManpower_Tmp1Table, sales_id.split("#")[1], "f", sales_id.split("#")[1], "m_qj_manpower_prem"));
                    sumStateMonthIssue.put(sales_id, sumActivity);
                    if (sumActivity >= issueSumStateMapThreshold) {
                        if (countStateMonthIssue.get(oldKey_IdCurrent) == null) {
                            Integer enter_order_monthManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, offLineKey_id, "f", offLineKey_id, "enter_order_month"));
                            countStateMonthIssue.put(oldKey_IdCurrent, enter_order_monthManpower - 1);
                        } else {
                            countStateMonthIssue.put(oldKey_IdCurrent, countStateMonthIssue.get(oldKey_IdCurrent) - 1);
                        }
                        countStateMonthIssue.put(key_id, Utils.int2IntIsNull(countStateMonthIssue.get(key_id)) + 1);
                    }
                }
            }else {
                if(sumStateMonthIssue.get(sales_id) != null){
                    Double sumActivity = sumStateMonthIssue.get(sales_id);
                    if(sumActivity >= issueSumStateMapThreshold){
                        countStateMonthIssue.put(oldKey_IdCurrent, countStateMonthIssue.get(oldKey_IdCurrent) - 1);
                        countStateMonthIssue.put(key_id, countStateMonthIssue.get(key_id) + 1);
                    }
                }
            }

            //月合格人力
            if(!"01".equals(currentDay.substring(8, 10))) {
                if (sumStateMonthQualified.get(sales_id) != null) {
                    Double sumActivity = sumStateMonthQualified.get(sales_id);
                    if (sumActivity >= qualifiedSumStateMapThreshold) {
                        if (countStateMonthQualified.get(oldKey_IdCurrent) == null) {
                            Integer eligible_manpower_monthManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, offLineKey_id, "f", offLineKey_id, "eligible_manpower_month"));
                            countStateMonthQualified.put(oldKey_IdCurrent, eligible_manpower_monthManpower - 1);
                        } else {
                            countStateMonthQualified.put(oldKey_IdCurrent, countStateMonthQualified.get(oldKey_IdCurrent) - 1);
                        }
                        countStateMonthQualified.put(key_id, Utils.int2IntIsNull(countStateMonthQualified.get(key_id)) + 1);
                    }
                } else {
                    Double sumActivity = Utils.str2DouIsNull(getHbaseValue(hTableManpower_Tmp1Table, sales_id.split("#")[1], "f", sales_id.split("#")[1], "m_qj_manpower_prem"));
                    sumStateMonthQualified.put(sales_id, sumActivity);
                    if (sumActivity >= qualifiedSumStateMapThreshold) {
                        if (countStateMonthQualified.get(oldKey_IdCurrent) == null) {
                            Integer eligible_manpower_monthManpower = Utils.str2IntIsNull(getHbaseValue(hTableManpowerTable, offLineKey_id, "f", offLineKey_id, "eligible_manpower_month"));
                            countStateMonthQualified.put(oldKey_IdCurrent, eligible_manpower_monthManpower - 1);
                        } else {
                            countStateMonthQualified.put(oldKey_IdCurrent, countStateMonthQualified.get(oldKey_IdCurrent) - 1);
                        }
                        countStateMonthQualified.put(key_id, Utils.int2IntIsNull(countStateMonthQualified.get(key_id)) + 1);
                    }
                }
            }else {
                if(sumStateMonthQualified.get(sales_id) != null){
                    Double sumActivity = sumStateMonthQualified.get(sales_id);
                    if(sumActivity >= issueSumStateMapThreshold){
                        countStateMonthQualified.put(oldKey_IdCurrent, countStateMonthQualified.get(oldKey_IdCurrent) - 1);
                        countStateMonthQualified.put(key_id, countStateMonthQualified.get(key_id) + 1);
                    }
                }
            }
        }


        /**算入职*/
        if (!"-".equals(rank)) {
            //日入职专员,累计入职专员
            if (Sets.newHashSet("A01", "A02", "A03", "A04", "A05", "A06", "A07", "A08", "A09").contains(rank)) {
                if (memberCountState.contains(key_id)) {
                    if ("1".equals(stat)) {
                        memberCountState.put(key_id, memberCountState.get(key_id) + 1);
                        memberCumulativeState.put(key_id, memberCumulativeState.get(key_id) + 1);
                    } else if ("2".equals(stat)) {
                        memberCumulativeState.put(key_id, memberCumulativeState.get(key_id) - 1);
                    }
                } else {
                    if ("1".equals(stat)) {
                        //日入职专员
                        memberCountState.put(key_id, 1);
                        //累计入职专员
                        memberCumulativeState.put(key_id, memberCumulativeState.get(key_id) + 1);
                    } else if ("2".equals(stat)) {
                        memberCumulativeState.put(key_id, memberCumulativeState.get(key_id) - 1);
                    }
                }
            }
            //日入职经理,累计入职经理
            else if (Sets.newHashSet("A10", "A11", "A12", "A13", "A14").contains(rank)) {
                if (managerCountState.contains(key_id)) {
                    if ("1".equals(stat)) {
                        managerCountState.put(key_id, managerCountState.get(key_id) + 1);
                        managerCumulativeState.put(key_id, managerCumulativeState.get(key_id) + 1);
                    } else if ("2".equals(stat)) {
                        managerCumulativeState.put(key_id, managerCumulativeState.get(key_id) - 1);
                    }
                } else {
                    if ("1".equals(stat)) {
                        //日入职经理
                        managerCountState.put(key_id, 1);
                        //累计入职经理
                        managerCumulativeState.put(key_id, managerCumulativeState.get(key_id) + 1);
                    } else if ("2".equals(stat)) {
                        managerCumulativeState.put(key_id, managerCumulativeState.get(key_id) - 1);
                    }
                }
            }
            //日入职总监,累计入职总监
            else if (Sets.newHashSet("A15", "A16", "A17", "A18", "A19").contains(rank)) {
                if (directorCountState.contains(key_id)) {
                    if ("1".equals(stat)) {
                        directorCountState.put(key_id, directorCountState.get(key_id) + 1);
                        directorCumulativeState.put(key_id, directorCumulativeState.get(key_id) + 1);
                    } else if ("2".equals(stat)) {
                        directorCumulativeState.put(key_id, directorCumulativeState.get(key_id) - 1);
                    }
                } else {
                    if ("1".equals(stat)) {
                        //日入职总监
                        directorCountState.put(key_id, 1);
                        //累计入职总监
                        directorCumulativeState.put(key_id, directorCumulativeState.get(key_id) + 1);
                    } else if ("2".equals(stat)) {
                        directorCumulativeState.put(key_id, directorCumulativeState.get(key_id) - 1);
                    }
                }
            }

            //月入职，季度入职，累计总人力
            if (Sets.newHashSet("A01", "A02", "A03", "A04", "A05", "A06", "A07", "A08", "A09", "A10", "A11", "A12", "A13", "A14", "A15", "A16", "A17", "A18", "A19").contains(rank)) {
                if ("1".equals(stat)) {
                    //月入职
                    totalMonthCumulativeState.put(key_id, totalMonthCumulativeState.get(key_id) + 1);
                    //季度入职
                    totalQuarterCumulativeState.put(key_id, totalQuarterCumulativeState.get(key_id) + 1);
                    //总人力
                    totalCumulativeState.put(key_id, totalCumulativeState.get(key_id) + 1);
                } else if ("2".equals(stat)) {
                    //月入职
                    totalMonthCumulativeState.put(key_id, totalMonthCumulativeState.get(key_id) - 1);
                    //季度入职
                    totalQuarterCumulativeState.put(key_id, totalQuarterCumulativeState.get(key_id) - 1);
                    //总人力
                    totalCumulativeState.put(key_id, totalCumulativeState.get(key_id) - 1);
                }
            }
        }

        /** 算人力 */
        else {
            /**日期缴活动人力 (当天出单signdate)  月期缴活动人力 (当月出单signdate)*/
            if("lcpol".equals(mark)){
                if (signdate.compareTo(currentDay) == 0) {
                    StateStorage.dayState(sumStateActivity, countStateActivity, key_id, sales_id, prem, activitySumStateMapThreshold);
                }
                if(signdate.compareTo(firstDayOfMonth) >= 0 && signdate.compareTo(endDayOfMonth) <= 0) {
                    StateStorage.monthActivityState(sumStateMonthActivity, countStateMonthActivity, key_id, sales_id, prem, hTableManpower_Tmp1Table, countStateMonthActivity.get(key_id), activitySumStateMapThreshold);
                }
            }

            if("lbpol".equals(mark)){
                if(signdate.compareTo(firstDayOfMonth) >= 0 && signdate.compareTo(endDayOfMonth) <= 0) {
                    StateStorage.monthActivityState(sumStateMonthActivity, countStateMonthActivity, key_id, sales_id, prem, hTableManpower_Tmp1Table, countStateMonthActivity.get(key_id), activitySumStateMapThreshold);
                }
            }


            /**日入职出单人力（当天入职probation_date，当天出单signdate)   月入职出单人力（当月入职probation_date，当月出单signdate)*/
            if("lcpol".equals(mark)){
                if (signdate.compareTo(currentDay) == 0 && probation_date.compareTo(currentDay) == 0) {
                    StateStorage.dayEnterState(sumStateIssue, countStateIssue, key_id, sales_id, prem, issueSumStateMapThreshold);
                }
                if(probation_date.compareTo(firstDayOfMonth) >= 0 && probation_date.compareTo(endDayOfMonth) <= 0 && signdate.compareTo(firstDayOfMonth) >= 0 && signdate.compareTo(endDayOfMonth) <= 0){
                    StateStorage.monthIssueState(sumStateMonthIssue, countStateMonthIssue, key_id, sales_id, prem, hTableManpower_Tmp1Table, countStateMonthIssue.get(key_id), issueSumStateMapThreshold);
                }
            }

            if("lbpol".equals(mark)){
                if(probation_date.compareTo(firstDayOfMonth) >= 0 && probation_date.compareTo(endDayOfMonth) <= 0 && signdate.compareTo(firstDayOfMonth) >= 0 && signdate.compareTo(endDayOfMonth) <= 0){
                    StateStorage.monthIssueState(sumStateMonthIssue, countStateMonthIssue, key_id, sales_id, prem, hTableManpower_Tmp1Table, countStateMonthIssue.get(key_id), issueSumStateMapThreshold);
                }
            }


            /**日合格人力(当天出单signdate)    月合格人力(当月出单signdate)*/
            if("lcpol".equals(mark)) {
                if (signdate.compareTo(currentDay) == 0) {
                    StateStorage.dayState(sumStateQualified, countStateQualified, key_id, sales_id, prem, qualifiedSumStateMapThreshold);
                }
                if(signdate.compareTo(firstDayOfMonth) >= 0 && signdate.compareTo(endDayOfMonth) <= 0) {
                    StateStorage.monthQualifiedState(sumStateMonthQualified, countStateMonthQualified, key_id, sales_id, prem, hTableManpower_Tmp1Table, countStateMonthQualified.get(key_id), qualifiedSumStateMapThreshold);
                }
            }

            if("lbpol".equals(mark)){
                if(signdate.compareTo(firstDayOfMonth) >= 0 && signdate.compareTo(endDayOfMonth) <= 0) {
                    StateStorage.monthQualifiedState(sumStateMonthQualified, countStateMonthQualified, key_id, sales_id, prem, hTableManpower_Tmp1Table, countStateMonthQualified.get(key_id), qualifiedSumStateMapThreshold);
                }
            }
        }

        //当日离职月期缴活动人力
        if("2".equals(stat)){
            //当日离职，当日没出单
            if(sumStateMonthIssue.get(sales_id) == null) {
                Double quitPerson = Utils.str2DouIsNull(getHbaseValue(hTableManpower_Tmp1Table, sales_id.split("#")[1], "f", sales_id.split("#")[1], "m_qj_manpower_prem"));
                if (quitPerson >= activitySumStateMapThreshold) {
                    if (quitState.contains("quitP")) {
                        quitState.put("quitP", quitState.get("quitP") + 1);
                    } else {
                        quitState.put("quitP", 1);
                    }
                }
            }else {
                //当天离职，当日出单
                if(sumStateMonthIssue.get(sales_id) >= activitySumStateMapThreshold){
                    if (quitState.contains("quitP")) {
                        quitState.put("quitP", quitState.get("quitP") + 1);
                    } else {
                        quitState.put("quitP", 1);
                    }
                }
            }
        }

        //当日离职月合格人力
        if("2".equals(stat)){
            //当日离职，当日没出单
            if(sumStateMonthQualified.get(sales_id) == null) {
                Double qualifiedPerson = Utils.str2DouIsNull(getHbaseValue(hTableManpower_Tmp1Table, sales_id.split("#")[1], "f", sales_id.split("#")[1], "m_qj_manpower_prem"));
                if (qualifiedPerson >= qualifiedSumStateMapThreshold) {
                    if (qualifiedState.contains("qualifiedP")) {
                        qualifiedState.put("qualifiedP", qualifiedState.get("qualifiedP") + 1);
                    } else {
                        qualifiedState.put("qualifiedP", 1);
                    }
                }
            }else {
                //当天离职，当日出单
                if(sumStateMonthQualified.get(sales_id) >= qualifiedSumStateMapThreshold){
                    if (qualifiedState.contains("qualifiedP")) {
                        qualifiedState.put("qualifiedP", qualifiedState.get("qualifiedP") + 1);
                    } else {
                        qualifiedState.put("qualifiedP", 1);
                    }
                }
            }
        }

        //保存当月累计期缴总保费
            if(sumStatePremium.get(key_id) == null){
                //离线机构月期缴保费
                Double hbaseValue = Utils.str2DouIsNull(getHbaseValue(hTableManpowerGeneralTable, offLineKey_id, "f", offLineKey_id, "regular_prem_month"));
                System.out.println("保存当月累计期缴总保费 : "+hbaseValue);
                StateStorage.sumStatePremium(sumStatePremium, key_id, Double.valueOf(String.format("%.2f", hbaseValue + prem )));
            }else {
                StateStorage.sumStatePremium(sumStatePremium, key_id, prem);
            }
        System.out.println("机构代码：" + key_id + "    " + "总保费：" + sumStatePremium.get(key_id));


        /**
         * 月期交人员活动率
         * 统计时点月期交人员活动人力/（统计期时点总人力-统计期时点总监人力-统计期时点月度入职人力+当月入职出单人力+当日离职月期缴活动人力）
         */
        //统计时点月期交人员活动人力
        Integer integer = Utils.int2IntIsNull(countStateMonthActivity.get(key_id));
        //统计期时点总人力
        Integer integer1 = Utils.int2IntIsNull(totalCumulativeState.get(key_id));
        //统计期时点总监人力
        Integer integer2 = Utils.int2IntIsNull(directorCumulativeState.get(key_id));
        //统计期时点月度入职人力
        Integer integer3 = Utils.int2IntIsNull(totalMonthCumulativeState.get(key_id));
        //当月入职出单人力
        Integer integer4 = Utils.int2IntIsNull(countStateMonthIssue.get(key_id));
        //当日离职月期缴活动人力
        Integer quitP = Utils.int2IntIsNull(quitState.get("quitP"));
        //（统计期时点总人力-统计期时点总监人力-统计期时点月度入职人力+当月入职出单人力）
        int integerSum = integer1 - integer2 - integer3 + integer4;
        //四舍五入并保留两位小数
        String monthActivityManpower = null;
        if(integerSum != 0) {
            monthActivityManpower = String.format("%.2f", ((double) integer / (integerSum + quitP)) * 100) + "%";
        }else {
            monthActivityManpower = "0%";
        }

        /**
         * 月合格人力占比
         * 统计时点月合格人力/（统计期时点总人力-统计期时点总监人力-统计期时点月度入职人力+当月入职出单人力+当日离职月合格人力）
         */
        //当日离职月合格人力
        Integer qualifiedP = Utils.int2IntIsNull(qualifiedState.get("qualifiedP"));
        String monthQualifiedManpower = null;
                Integer integer5 = Utils.int2IntIsNull(countStateMonthQualified.get(key_id));
        if(integerSum != 0) {
            //System.out.println(integer5 +"  " + integer1 +"  " + integer2 +"  " + integer3 +"  " + integer4);
            monthQualifiedManpower = String.format("%.2f", ((double) integer5 / (integerSum + qualifiedP)) * 100) + "%";
        }else {
            monthQualifiedManpower = "0%";
        }
        /**
         * 当月人均产能
         * 当月累计期交保费/月度期交活动人力*100%
         */
        String averageCapacity = null;
                Double aDouble = sumStatePremium.get(key_id) != null ? sumStatePremium.get(key_id) : 0.00;
        if(aDouble > 0) {
            averageCapacity = String.format("%.2f", (aDouble / integer) * 100) + "%";
        }else {
            averageCapacity = "0%";
        }

        System.out.println("日期交活动人力保费：" + sumStateActivity.get(sales_id));
        System.out.println("日入职出单人力保费：" + sumStateIssue.get(sales_id));
        System.out.println("日合格人力：" + sumStateQualified.get(sales_id));
        System.out.println("月期交活动人力保费："+sumStateMonthActivity.get(sales_id));
        System.out.println("月入职出单人力保费：" + sumStateMonthIssue.get(sales_id));
        System.out.println("月合格人力保费：" + sumStateMonthQualified.get(sales_id));
        cp.setKey_id(key_id);//主键
        cp.setDay_id(key_id.split("#")[0]);//日期
        cp.setManage_code(key_id.split("#")[1]);//机构(分公司)
        cp.setManage_name(branch_name);
        //日入职
//        cp.setDirector_enter__day(directorCountState.get(key_id) != null ? directorCountState.get(key_id).toString() : "0");//日入职总监人数
        cp.setDirector_enter__day(Utils.int2StrIsNull(directorCountState.get(key_id)));//日入职总监人数
        cp.setManager_enter__day(Utils.int2StrIsNull(managerCountState.get(key_id)));//日入职部经理人数
        cp.setMember_enter__day(Utils.int2StrIsNull(memberCountState.get(key_id)));//日入职专员人数
        //累计入职
        cp.setTotal_director(Utils.int2StrIsNull(directorCumulativeState.get(key_id)));//累计总监人数
        cp.setTotal_manager(Utils.int2StrIsNull(managerCumulativeState.get(key_id)));//累计部经理人数
        cp.setTotal_member(Utils.int2StrIsNull(memberCumulativeState.get(key_id)));//累计专员人数
        //月，季度，总累计
        cp.setNew_manpower_month(Utils.int2StrIsNull(totalMonthCumulativeState.get(key_id)));//月入职人力
        cp.setNew_manpower_quarter(Utils.int2StrIsNull(totalQuarterCumulativeState.get(key_id)));//季度入职人力
        cp.setTotal_manpower(Utils.int2StrIsNull(totalCumulativeState.get(key_id)));//累计总人力

        cp.setIncrease_quarter_plan("0");//季度增员目标
        //日活动，出单，合格
        cp.setActivity_manpower_day(Utils.int2StrIsNull(countStateActivity.get(key_id)));//日期交活动人力
        cp.setEnter_order_day(Utils.int2StrIsNull(countStateIssue.get(key_id)));//日入职出单人力（期交)
        cp.setEligible_manpower_day(Utils.int2StrIsNull(countStateQualified.get(key_id)));//日合格人力
        //月活动，出单，合格
        cp.setActivity_manpower_month(Utils.int2StrIsNull(countStateMonthActivity.get(key_id)));//月期交活动人力
        cp.setEnter_order_month(Utils.int2StrIsNull(countStateMonthIssue.get(key_id)));//当月入职出单人力（期交)
        cp.setEligible_manpower_month(Utils.int2StrIsNull(countStateMonthQualified.get(key_id)));//月合格人力

        cp.setActivity_rate_month(monthActivityManpower);//月期交人员活动率
        cp.setEligible_manpower_ratio(monthQualifiedManpower);//月合格人力占比
        cp.setProductivity_month(averageCapacity);//当月人均产能

        cp.setLoad_date(DateUDF.getCurrentDate());

        return cp;
    }
}
