package com.pactera.yhl.apps.construction.statejob;

import com.google.common.collect.Sets;
import com.pactera.yhl.apps.construction.entity.ConstructionPOJO;
import com.pactera.yhl.apps.construction.entity.PremiumsPOJO;
import com.pactera.yhl.apps.construction.util.*;
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
 * @description:  中间状态计算层(总公司)
 */
public class TotalMergeCountStateMap extends RichMapFunction<Tuple2<String, PremiumsPOJO>, ConstructionPOJO> {

    //日入职
    MapState<String, Integer> memberCountStateTotal;//日入职专员
    MapState<String, Integer> managerCountStateTotal;//日入职经理
    MapState<String, Integer> directorCountStateTotal;//日入职总监
    //累计入职
    MapState<String, Integer> memberCumulativeStateTotal;//累计入职专员
    MapState<String, Integer> managerCumulativeStateTotal;//累计入职经理
    MapState<String, Integer> directorCumulativeStateTotal;//累计入职总监
    //月入职，季度入职，累计总人力
    MapState<String, Integer> totalMonthCumulativeState;//月入职
    MapState<String, Integer> totalQuarterCumulativeStateTotal;//季度入职
    MapState<String, Integer> totalCumulativeStateTotal;//累计总人力

    //日期缴活动人力
    MapState<String, Double> sumStateActivityTotal;
    MapState<String, Integer> countStateActivityTotal;
    final static double activitySumStateMapThreshold = 10000.00;
    MapState<String, Double> sumStateMonthActivityTotal;
    MapState<String, Integer> countStateMonthActivityTotal;

    //日入职出单人力
    MapState<String, Double> sumStateIssueTotal;
    MapState<String, Integer> countStateIssueTotal;
    final static double issueSumStateMapThreshold = 0.00;
    MapState<String, Double> sumStateMonthIssueTotal;
    MapState<String, Integer> countStateMonthIssueTotal;

    //日合格人力
    MapState<String, Double> sumStateQualifiedTotal;
    MapState<String, Integer> countStateQualifiedTotal;
    final static double qualifiedSumStateMapThreshold = 30000.00;
    MapState<String, Double> sumStateMonthQualifiedTotal;
    MapState<String, Integer> countStateMonthQualifiedTotal;

    //当月累计期缴保费
    MapState<String, Double> sumStatePremiumTotal;

    //当日离职月期缴活动人力
    MapState<String, Integer> quitStateTotal;
    //当日离职月合格人力
    MapState<String, Integer> qualifiedStateTotal;

    final static String manpowerTable = "kl_base:application_manpower_result";//离线人力建设数据
    final static String generalTable = "kl_base:application_general_result";//离线机构月期缴保费
    final static String manpower_tmp1Table = "kl_base:application_manpower_result_tmp1";//离线人员期缴保费
    private ConstructionPOJO cpTotal = new ConstructionPOJO();
    private Connection connectionTotal = null;
    private HTable hTableManpowerTableTotal =null;
    private HTable hTableManpowerGeneralTableTotal =null;
    private HTable hTableManpower_Tmp1TableTotal =null;

    @Override
    public void open(Configuration parameters) throws IOException {

        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String config_path = params.get("config_path");
        connectionTotal = MyHbaseCli.hbaseConnection(config_path);
        try{
            //离线人力建设数据
            hTableManpowerTableTotal = (HTable) connectionTotal.getTable(TableName.valueOf(manpowerTable));
            //离线机构月期缴保费
            hTableManpowerGeneralTableTotal = (HTable) connectionTotal.getTable(TableName.valueOf(generalTable));
            //离线人员期缴保费
            hTableManpower_Tmp1TableTotal = (HTable) connectionTotal.getTable(TableName.valueOf(manpower_tmp1Table));
        }catch (Exception e){
            e.printStackTrace();
        }

        //日入职
        memberCountStateTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "memberCountStateTotal", String.class, Integer.class
        ));

        managerCountStateTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "managerCountStateTotal", String.class, Integer.class
        ));

        directorCountStateTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "directorCountStateTotal", String.class, Integer.class
        ));

        //累计入职
        memberCumulativeStateTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "memberCumulativeStateTotal", String.class, Integer.class
        ));

        managerCumulativeStateTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "managerCumulativeStateTotal", String.class, Integer.class
        ));

        directorCumulativeStateTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "directorCumulativeStateTotal", String.class, Integer.class
        ));

        //日期缴活动人力，日入职出单人力，日合格人力
        sumStateActivityTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>(
                "sumStateActivityTotal", String.class, Double.class
        ));

        countStateActivityTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "countStateActivityTotal", String.class, Integer.class
        ));

        sumStateIssueTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>(
                "sumStateIssueTotal", String.class, Double.class
        ));

        countStateIssueTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "countStateIssueTotal", String.class, Integer.class
        ));

        sumStateQualifiedTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>(
                "sumStateQualifiedTotal", String.class, Double.class
        ));
        countStateQualifiedTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "countStateQualifiedTotal", String.class, Integer.class
        ));

        //月期缴活动人力，月入职出单人力，月合格人力
        sumStateMonthActivityTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>(
                "sumStateMonthActivityTotal", String.class, Double.class
        ));
        countStateMonthActivityTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "countStateMonthActivityTotal", String.class, Integer.class
        ));

        sumStateMonthIssueTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>(
                "sumStateMonthIssueTotal", String.class, Double.class
        ));

        countStateMonthIssueTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
            "countStateMonthIssueTotal", String.class, Integer.class
        ));

        sumStateMonthQualifiedTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>(
                "sumStateMonthQualifiedTotal", String.class, Double.class
        ));

        countStateMonthQualifiedTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
            "countStateMonthQualifiedTotal", String.class, Integer.class
        ));

        //月，季度，总累计
        totalMonthCumulativeState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "totalMonthCumulativeState", String.class, Integer.class
        ));

        totalQuarterCumulativeStateTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "totalQuarterCumulativeStateTotal", String.class, Integer.class
        ));

        totalCumulativeStateTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "totalCumulativeStateTotal", String.class, Integer.class
        ));

        //期缴总保费
        sumStatePremiumTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>(
                "sumStatePremiumTotal", String.class, Double.class
        ));

        //当日离职月期缴活动人力
        quitStateTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "quitStateTotal", String.class, Integer.class
        ));

        //当日离职月合格人力
        qualifiedStateTotal = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "qualifiedStateTotal", String.class, Integer.class
        ));
    }

    public ConstructionPOJO map(Tuple2<String, PremiumsPOJO> value) throws Exception {

        String firstDayOfMonth = TotalDateUDF.getFirstDayOfMonthTotal();
        String endDayOfMonth = TotalDateUDF.getEndDayOfMonthTotal();
        String currentDay = TotalDateUDF.getCurrentDayTotal();
        String beforeDay = TotalDateUDF.getbeforeDayTotal();

        String key_id = value.f0;
        System.out.println("key_id"+key_id);
        String rank = value.f1.getRank();
        String branch_name = "总公司";
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
            Integer new_manpower_monthManpower = TotalUtils.str2IntIsNullTotal(getHbaseValue(hTableManpowerTableTotal, offLineKey_id, "f", offLineKey_id, "new_manpower_month"));
            System.out.println("月入职:" + new_manpower_monthManpower);
            totalMonthCumulativeState.put(key_id, new_manpower_monthManpower);
        }
        //季度入职
        if(totalQuarterCumulativeStateTotal.get(key_id) == null) {
            Integer new_manpower_quarterManpower = TotalUtils.str2IntIsNullTotal(getHbaseValue(hTableManpowerTableTotal, offLineKey_id, "f", offLineKey_id, "new_manpower_quarter"));
            System.out.println("季度入职:" + new_manpower_quarterManpower);
            totalQuarterCumulativeStateTotal.put(key_id, new_manpower_quarterManpower);
        }
        if(memberCumulativeStateTotal.get(key_id) == null) {
            //累计入职专员
            Integer total_memberManpower = TotalUtils.str2IntIsNullTotal(getHbaseValue(hTableManpowerTableTotal, offLineKey_id, "f", offLineKey_id, "total_member"));
            System.out.println("累计入职专员:" + total_memberManpower);
            memberCumulativeStateTotal.put(key_id, total_memberManpower);
        }

        //累计入职经理
        if(managerCumulativeStateTotal.get(key_id) == null) {
            Integer total_managerManpower = TotalUtils.str2IntIsNullTotal(getHbaseValue(hTableManpowerTableTotal, offLineKey_id, "f", offLineKey_id, "total_manager"));
            System.out.println("累计入职经理:" + total_managerManpower);
            managerCumulativeStateTotal.put(key_id, total_managerManpower);
        }
            //累计入职总监
        if(directorCumulativeStateTotal.get(key_id) == null) {
            Integer total_directorManpower = TotalUtils.str2IntIsNullTotal(getHbaseValue(hTableManpowerTableTotal, offLineKey_id, "f", offLineKey_id, "total_director"));
            System.out.println("累计入职总监:" + total_directorManpower);
            directorCumulativeStateTotal.put(key_id, total_directorManpower);
        }
        //累计总人力
        if(totalCumulativeStateTotal.get(key_id) == null) {
            Integer total_manpowerManpower = TotalUtils.str2IntIsNullTotal(getHbaseValue(hTableManpowerTableTotal, offLineKey_id, "f", offLineKey_id, "total_manpower"));
            System.out.println("累计总人力:" + total_manpowerManpower);
            totalCumulativeStateTotal.put(key_id, total_manpowerManpower);
            System.out.println("人员变动总人力:"+totalCumulativeStateTotal.get(key_id));
        }
        //月期缴活动人力
        if(countStateMonthActivityTotal.get(key_id) == null) {
            if("01".equals(currentDay.substring(8, 10))) {
                countStateMonthActivityTotal.put(key_id, 0);
            }else {
                Integer activity_manpower_monthManpower = TotalUtils.str2IntIsNullTotal(getHbaseValue(hTableManpowerTableTotal, offLineKey_id, "f", offLineKey_id, "activity_manpower_month"));
                System.out.println("月期缴活动人力:" + activity_manpower_monthManpower);
                countStateMonthActivityTotal.put(key_id, activity_manpower_monthManpower);
            }
        }
        //月入职出单人力
        if(countStateMonthIssueTotal.get(key_id) == null) {
            if("01".equals(currentDay.substring(8, 10))) {
                countStateMonthIssueTotal.put(key_id, 0);
            }else {
                Integer enter_order_monthManpower = TotalUtils.str2IntIsNullTotal(getHbaseValue(hTableManpowerTableTotal, offLineKey_id, "f", offLineKey_id, "enter_order_month"));
                System.out.println("月入职出单人力:" + enter_order_monthManpower);
                countStateMonthIssueTotal.put(key_id, enter_order_monthManpower);
            }
        }
        //月合格人力
        if(countStateMonthQualifiedTotal.get(key_id) == null) {
            if("01".equals(currentDay.substring(8, 10))) {
                countStateMonthQualifiedTotal.put(key_id, 0);
            }else {
                Integer eligible_manpower_monthManpower = TotalUtils.str2IntIsNullTotal(getHbaseValue(hTableManpowerTableTotal, offLineKey_id, "f", offLineKey_id, "eligible_manpower_month"));
                System.out.println("月合格人力:" + eligible_manpower_monthManpower);
                countStateMonthQualifiedTotal.put(key_id, eligible_manpower_monthManpower);
            }
        }


        /**算入职*/
        if (!"-".equals(rank)) {
            //日入职专员,累计入职专员
            if (Sets.newHashSet("A01", "A02", "A03", "A04", "A05", "A06", "A07", "A08", "A09").contains(rank)) {
                if (memberCountStateTotal.contains(key_id)) {
                    if ("1".equals(stat)) {
                        memberCountStateTotal.put(key_id, memberCountStateTotal.get(key_id) + 1);
                        memberCumulativeStateTotal.put(key_id, memberCumulativeStateTotal.get(key_id) + 1);
                    } else if ("2".equals(stat)) {
                        memberCumulativeStateTotal.put(key_id, memberCumulativeStateTotal.get(key_id) - 1);
                    }
                } else {
                    if ("1".equals(stat)) {
                        //日入职专员
                        memberCountStateTotal.put(key_id, 1);
                        //累计入职专员
                        memberCumulativeStateTotal.put(key_id, memberCumulativeStateTotal.get(key_id) + 1);
                    } else if ("2".equals(stat)) {
                        memberCumulativeStateTotal.put(key_id, memberCumulativeStateTotal.get(key_id) - 1);
                    }
                }
            }
            //日入职经理,累计入职经理
            else if (Sets.newHashSet("A10", "A11", "A12", "A13", "A14").contains(rank)) {
                if (managerCountStateTotal.contains(key_id)) {
                    if ("1".equals(stat)) {
                        managerCountStateTotal.put(key_id, managerCountStateTotal.get(key_id) + 1);
                        managerCumulativeStateTotal.put(key_id, managerCumulativeStateTotal.get(key_id) + 1);
                    } else if ("2".equals(stat)) {
                        managerCumulativeStateTotal.put(key_id, managerCumulativeStateTotal.get(key_id) - 1);
                    }
                } else {
                    if ("1".equals(stat)) {
                        //日入职经理
                        managerCountStateTotal.put(key_id, 1);
                        //累计入职经理
                        managerCumulativeStateTotal.put(key_id, managerCumulativeStateTotal.get(key_id) + 1);
                    } else if ("2".equals(stat)) {
                        managerCumulativeStateTotal.put(key_id, managerCumulativeStateTotal.get(key_id) - 1);
                    }
                }
            }
            //日入职总监,累计入职总监
            else if (Sets.newHashSet("A15", "A16", "A17", "A18", "A19").contains(rank)) {
                if (directorCountStateTotal.contains(key_id)) {
                    if ("1".equals(stat)) {
                        directorCountStateTotal.put(key_id, directorCountStateTotal.get(key_id) + 1);
                        directorCumulativeStateTotal.put(key_id, directorCumulativeStateTotal.get(key_id) + 1);
                    } else if ("2".equals(stat)) {
                        directorCumulativeStateTotal.put(key_id, directorCumulativeStateTotal.get(key_id) - 1);
                    }
                } else {
                    if ("1".equals(stat)) {
                        //日入职总监
                        directorCountStateTotal.put(key_id, 1);
                        //累计入职总监
                        directorCumulativeStateTotal.put(key_id, directorCumulativeStateTotal.get(key_id) + 1);
                    } else if ("2".equals(stat)) {
                        directorCumulativeStateTotal.put(key_id, directorCumulativeStateTotal.get(key_id) - 1);
                    }
                }
            }

            //月入职，季度入职，累计总人力
            if (Sets.newHashSet("A01", "A02", "A03", "A04", "A05", "A06", "A07", "A08", "A09", "A10", "A11", "A12", "A13", "A14", "A15", "A16", "A17", "A18", "A19").contains(rank)) {
                if ("1".equals(stat)) {
                    //月入职
                    totalMonthCumulativeState.put(key_id, totalMonthCumulativeState.get(key_id) + 1);
                    //季度入职
                    totalQuarterCumulativeStateTotal.put(key_id, totalQuarterCumulativeStateTotal.get(key_id) + 1);
                    //总人力
                    totalCumulativeStateTotal.put(key_id, totalCumulativeStateTotal.get(key_id) + 1);
                } else if ("2".equals(stat)) {
                    //月入职
                    totalMonthCumulativeState.put(key_id, totalMonthCumulativeState.get(key_id) - 1);
                    //季度入职
                    totalQuarterCumulativeStateTotal.put(key_id, totalQuarterCumulativeStateTotal.get(key_id) - 1);
                    //总人力
                    totalCumulativeStateTotal.put(key_id, totalCumulativeStateTotal.get(key_id) - 1);
                }
            }
        }

        /** 算人力 */
        else {
            /**日期缴活动人力 (当天出单signdate)  月期缴活动人力 (当月出单signdate)*/
            if("lcpol".equals(mark)){
                if (signdate.compareTo(currentDay) == 0) {
                    TotalStateStorage.dayStateTotal(sumStateActivityTotal, countStateActivityTotal, key_id, sales_id, prem, activitySumStateMapThreshold);
                }
                if(signdate.compareTo(firstDayOfMonth) >= 0 && signdate.compareTo(endDayOfMonth) <= 0) {
                    TotalStateStorage.monthActivityStateTotal(sumStateMonthActivityTotal, countStateMonthActivityTotal, key_id, sales_id, prem, hTableManpower_Tmp1TableTotal, countStateMonthActivityTotal.get(key_id), activitySumStateMapThreshold);
                }
            }

            if("lbpol".equals(mark)){
                if(signdate.compareTo(firstDayOfMonth) >= 0 && signdate.compareTo(endDayOfMonth) <= 0) {
                    TotalStateStorage.monthActivityStateTotal(sumStateMonthActivityTotal, countStateMonthActivityTotal, key_id, sales_id, prem, hTableManpower_Tmp1TableTotal, countStateMonthActivityTotal.get(key_id), activitySumStateMapThreshold);
                }
            }

            /**日入职出单人力（当天入职probation_date，当天出单signdate)   月入职出单人力（当月入职probation_date，当月出单signdate)*/
            if("lcpol".equals(mark)){
                if (signdate.compareTo(currentDay) == 0 && probation_date.compareTo(currentDay) == 0) {
                    TotalStateStorage.dayEnterStateTotal(sumStateIssueTotal, countStateIssueTotal, key_id, sales_id, prem, issueSumStateMapThreshold);
                }
                if(probation_date.compareTo(firstDayOfMonth) >= 0 && probation_date.compareTo(endDayOfMonth) <= 0 && signdate.compareTo(firstDayOfMonth) >= 0 && signdate.compareTo(endDayOfMonth) <= 0){
                    TotalStateStorage.monthIssueStateTotal(sumStateMonthIssueTotal, countStateMonthIssueTotal, key_id, sales_id, prem, hTableManpower_Tmp1TableTotal, countStateMonthIssueTotal.get(key_id), issueSumStateMapThreshold);
                }
            }

            if("lbpol".equals(mark)){
                if(probation_date.compareTo(firstDayOfMonth) >= 0 && probation_date.compareTo(endDayOfMonth) <= 0 && signdate.compareTo(firstDayOfMonth) >= 0 && signdate.compareTo(endDayOfMonth) <= 0){
                    TotalStateStorage.monthIssueStateTotal(sumStateMonthIssueTotal, countStateMonthIssueTotal, key_id, sales_id, prem, hTableManpower_Tmp1TableTotal, countStateMonthIssueTotal.get(key_id), issueSumStateMapThreshold);
                }
            }

            /**日合格人力(当天出单signdate)    月合格人力(当月出单signdate)*/
            if("lcpol".equals(mark)) {
                if (signdate.compareTo(currentDay) == 0) {
                    TotalStateStorage.dayStateTotal(sumStateQualifiedTotal, countStateQualifiedTotal, key_id, sales_id, prem, qualifiedSumStateMapThreshold);
                }
                if(signdate.compareTo(firstDayOfMonth) >= 0 && signdate.compareTo(endDayOfMonth) <= 0) {
                    TotalStateStorage.monthQualifiedStateTotal(sumStateMonthQualifiedTotal, countStateMonthQualifiedTotal, key_id, sales_id, prem, hTableManpower_Tmp1TableTotal, countStateMonthQualifiedTotal.get(key_id), qualifiedSumStateMapThreshold);
                }
            }

            if("lbpol".equals(mark)){
                if(signdate.compareTo(firstDayOfMonth) >= 0 && signdate.compareTo(endDayOfMonth) <= 0) {
                    TotalStateStorage.monthQualifiedStateTotal(sumStateMonthQualifiedTotal, countStateMonthQualifiedTotal, key_id, sales_id, prem, hTableManpower_Tmp1TableTotal, countStateMonthQualifiedTotal.get(key_id), qualifiedSumStateMapThreshold);
                }
            }
        }

        //当日离职月期缴活动人力
        if("2".equals(stat)){
            //当日离职，当日没出单
            if(sumStateMonthIssueTotal.get(sales_id) == null) {
                Double quitPerson = TotalUtils.str2DouIsNullTotal(getHbaseValue(hTableManpower_Tmp1TableTotal, sales_id.split("#")[1], "f", sales_id.split("#")[1], "m_qj_manpower_prem"));
                if (quitPerson >= activitySumStateMapThreshold) {
                    if (quitStateTotal.contains("quitP")) {
                        quitStateTotal.put("quitP", quitStateTotal.get("quitP") + 1);
                    } else {
                        quitStateTotal.put("quitP", 1);
                    }
                }
            }else {
                //当天离职，当日出单
                if(sumStateMonthIssueTotal.get(sales_id) >= activitySumStateMapThreshold){
                    if (quitStateTotal.contains("quitP")) {
                        quitStateTotal.put("quitP", quitStateTotal.get("quitP") + 1);
                    } else {
                        quitStateTotal.put("quitP", 1);
                    }
                }
            }
        }

        //当日离职月合格人力
        if("2".equals(stat)){
            //当日离职，当日没出单
            if(sumStateMonthQualifiedTotal.get(sales_id) == null) {
                Double qualifiedPerson = TotalUtils.str2DouIsNullTotal(getHbaseValue(hTableManpower_Tmp1TableTotal, sales_id.split("#")[1], "f", sales_id.split("#")[1], "m_qj_manpower_prem"));
                if (qualifiedPerson >= qualifiedSumStateMapThreshold) {
                    if (qualifiedStateTotal.contains("qualifiedP")) {
                        qualifiedStateTotal.put("qualifiedP", qualifiedStateTotal.get("qualifiedP") + 1);
                    } else {
                        qualifiedStateTotal.put("qualifiedP", 1);
                    }
                }
            }else {
                //当天离职，当日出单
                if(sumStateMonthQualifiedTotal.get(sales_id) >= qualifiedSumStateMapThreshold){
                    if (qualifiedStateTotal.contains("qualifiedP")) {
                        qualifiedStateTotal.put("qualifiedP", qualifiedStateTotal.get("qualifiedP") + 1);
                    } else {
                        qualifiedStateTotal.put("qualifiedP", 1);
                    }
                }
            }
        }

        //保存当月累计期缴总保费
            if(sumStatePremiumTotal.get(key_id) == null){
                //离线机构月期缴保费
                Double hbaseValue = TotalUtils.str2DouIsNullTotal(getHbaseValue(hTableManpowerGeneralTableTotal, offLineKey_id, "f", offLineKey_id, "regular_prem_month"));
//                System.out.println("hbaseValue:::"+hbaseValue);
                TotalStateStorage.sumStatePremiumTotal(sumStatePremiumTotal, key_id, Double.valueOf(String.format("%.2f", hbaseValue + prem)));
            }else {
                TotalStateStorage.sumStatePremiumTotal(sumStatePremiumTotal, key_id, prem);
            }
        System.out.println("机构代码：" + key_id + "    " + "总保费：" + sumStatePremiumTotal.get(key_id));


        /**
         * 月期交人员活动率
         * 统计时点月期交人员活动人力/（统计期时点总人力-统计期时点总监人力-统计期时点月度入职人力+当月入职出单人力+当日离职月期缴活动人力）
         */
        //统计时点月期交人员活动人力
        Integer integer = TotalUtils.int2IntIsNullTotal(countStateMonthActivityTotal.get(key_id));
        //统计期时点总人力
        Integer integer1 = TotalUtils.int2IntIsNullTotal(totalCumulativeStateTotal.get(key_id));
        //统计期时点总监人力
        Integer integer2 = TotalUtils.int2IntIsNullTotal(directorCumulativeStateTotal.get(key_id));
        //统计期时点月度入职人力
        Integer integer3 = TotalUtils.int2IntIsNullTotal(totalMonthCumulativeState.get(key_id));
        //当月入职出单人力
        Integer integer4 = TotalUtils.int2IntIsNullTotal(countStateMonthIssueTotal.get(key_id));
        //当日离职月期缴活动人力
        Integer quitP = TotalUtils.int2IntIsNullTotal(quitStateTotal.get("quitP"));
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
        Integer qualifiedP = TotalUtils.int2IntIsNullTotal(qualifiedStateTotal.get("qualifiedP"));
        String monthQualifiedManpower = null;
                Integer integer5 = TotalUtils.int2IntIsNullTotal(countStateMonthQualifiedTotal.get(key_id));
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
                Double aDouble = sumStatePremiumTotal.get(key_id) != null ? sumStatePremiumTotal.get(key_id) : 0.00;
        if(aDouble > 0) {
            averageCapacity = String.format("%.2f", (aDouble / integer) * 100) + "%";
        }else {
            averageCapacity = "0%";
        }

        System.out.println("日期交活动人力保费：" + sumStateActivityTotal.get(sales_id));
        System.out.println("日入职出单人力保费：" + sumStateIssueTotal.get(sales_id));
        System.out.println("日合格人力：" + sumStateQualifiedTotal.get(sales_id));
        System.out.println("月期交活动人力保费："+sumStateMonthActivityTotal.get(sales_id));
        System.out.println("月入职出单人力保费：" + sumStateMonthIssueTotal.get(sales_id));
        System.out.println("月合格人力：" + sumStateMonthQualifiedTotal.get(sales_id));
        cpTotal.setKey_id(key_id);//主键
        cpTotal.setDay_id(key_id.split("#")[0]);//日期
        cpTotal.setManage_code(key_id.split("#")[1]);//机构(分公司)
        cpTotal.setManage_name(branch_name);
        //日入职
//        cp.setDirector_enter__day(directorCountStateTotal.get(key_id) != null ? directorCountStateTotal.get(key_id).toString() : "0");//日入职总监人数
        cpTotal.setDirector_enter__day(TotalUtils.int2StrIsNullTotal(directorCountStateTotal.get(key_id)));//日入职总监人数
        cpTotal.setManager_enter__day(TotalUtils.int2StrIsNullTotal(managerCountStateTotal.get(key_id)));//日入职部经理人数
        cpTotal.setMember_enter__day(TotalUtils.int2StrIsNullTotal(memberCountStateTotal.get(key_id)));//日入职专员人数
        //累计入职
        cpTotal.setTotal_director(TotalUtils.int2StrIsNullTotal(directorCumulativeStateTotal.get(key_id)));//累计总监人数
        cpTotal.setTotal_manager(TotalUtils.int2StrIsNullTotal(managerCumulativeStateTotal.get(key_id)));//累计部经理人数
        cpTotal.setTotal_member(TotalUtils.int2StrIsNullTotal(memberCumulativeStateTotal.get(key_id)));//累计专员人数
        //月，季度，总累计
        cpTotal.setNew_manpower_month(TotalUtils.int2StrIsNullTotal(totalMonthCumulativeState.get(key_id)));//月入职人力
        cpTotal.setNew_manpower_quarter(TotalUtils.int2StrIsNullTotal(totalQuarterCumulativeStateTotal.get(key_id)));//季度入职人力
        cpTotal.setTotal_manpower(TotalUtils.int2StrIsNullTotal(totalCumulativeStateTotal.get(key_id)));//累计总人力

        cpTotal.setIncrease_quarter_plan("0");//季度增员目标
        //日活动，出单，合格
        cpTotal.setActivity_manpower_day(TotalUtils.int2StrIsNullTotal(countStateActivityTotal.get(key_id)));//日期交活动人力
        cpTotal.setEnter_order_day(TotalUtils.int2StrIsNullTotal(countStateIssueTotal.get(key_id)));//日入职出单人力（期交)
        cpTotal.setEligible_manpower_day(TotalUtils.int2StrIsNullTotal(countStateQualifiedTotal.get(key_id)));//日合格人力
        //月活动，出单，合格
        cpTotal.setActivity_manpower_month(TotalUtils.int2StrIsNullTotal(countStateMonthActivityTotal.get(key_id)));//月期交活动人力
        cpTotal.setEnter_order_month(TotalUtils.int2StrIsNullTotal(countStateMonthIssueTotal.get(key_id)));//当月入职出单人力（期交)
        cpTotal.setEligible_manpower_month(TotalUtils.int2StrIsNullTotal(countStateMonthQualifiedTotal.get(key_id)));//月合格人力

        cpTotal.setActivity_rate_month(monthActivityManpower);//月期交人员活动率
        cpTotal.setEligible_manpower_ratio(monthQualifiedManpower);//月合格人力占比
        cpTotal.setProductivity_month(averageCapacity);//当月人均产能

        cpTotal.setLoad_date(DateUDF.getCurrentDate());

        return cpTotal;
    }
}
