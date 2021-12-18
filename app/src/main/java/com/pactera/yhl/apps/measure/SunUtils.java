package com.pactera.yhl.apps.measure;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/11/16 15:57
 */
public class SunUtils {
    public static Map<String, Tuple3<Double,Double,Tuple2<Double,Double>>> map = new HashMap<>();

    static {
        //专员指标
        map.put("A01",Tuple3.of(90000.0,180000.0,Tuple2.of(0.0,0.0)));
        map.put("A02",Tuple3.of(180000.0,270000.0,Tuple2.of(0.0,0.0)));
        map.put("A03",Tuple3.of(270000.0,360000.0,Tuple2.of(0.0,0.0)));
        map.put("A04",Tuple3.of(360000.0,450000.0,Tuple2.of(0.0,0.0)));
        map.put("A05",Tuple3.of(450000.0,540000.0,Tuple2.of(0.0,0.0)));
        map.put("A06",Tuple3.of(540000.0,630000.0,Tuple2.of(0.0,0.0)));
        map.put("A07",Tuple3.of(630000.0,720000.0,Tuple2.of(0.0,0.0)));
        map.put("A08",Tuple3.of(720000.0,810000.0,Tuple2.of(0.0,0.0)));
        map.put("A09",Tuple3.of(810000.0,0.0,Tuple2.of(0.0,0.0)));
        //部经理
        map.put("A10",Tuple3.of(900000.0,1200000.0,Tuple2.of(0.5,0.8)));
        map.put("A11",Tuple3.of(1200000.0,1800000.0,Tuple2.of(0.5,0.8)));
        map.put("A12",Tuple3.of(1800000.0,2400000.0,Tuple2.of(0.5,0.8)));
        map.put("A13",Tuple3.of(2400000.0,3000000.0,Tuple2.of(0.5,0.8)));
        map.put("A14",Tuple3.of(3000000.0,0.0,Tuple2.of(0.5,0.8)));
        //区域总监
        map.put("A15",Tuple3.of(3600000.0,4200000.0,Tuple2.of(0.5,0.8)));
        map.put("A16",Tuple3.of(4200000.0,4800000.0,Tuple2.of(0.5,0.8)));
        map.put("A17",Tuple3.of(4800000.0,5700000.0,Tuple2.of(0.5,0.8)));
        map.put("A18",Tuple3.of(5700000.0,6600000.0,Tuple2.of(0.5,0.8)));
        map.put("A19",Tuple3.of(6600000.0,0.0,Tuple2.of(0.5,0.8)));
    }


    //传进产品代码，返回折扣率
//    public static double ConversionRate(String product_code){
//        if("C174".equalsIgnoreCase(product_code) ||
//                "I168".equalsIgnoreCase(product_code) ||
//                "I165".equalsIgnoreCase(product_code) ||
//                "I138".equalsIgnoreCase(product_code) ||
//                "I139".equalsIgnoreCase(product_code) ||
//                "I161".equalsIgnoreCase(product_code)){
//            rate = 1.2;
//        }else if("YB717".equalsIgnoreCase(product_code) ||
//                "YB716".equalsIgnoreCase(product_code) ||
//                "C173".equalsIgnoreCase(product_code) ||
//                "YB721".equalsIgnoreCase(product_code)){
//            rate = 1.0;
//        }else if("YB705".equalsIgnoreCase(product_code) ||
//                "YB711".equalsIgnoreCase(product_code) ||
//                "YB719".equalsIgnoreCase(product_code)){
//            rate = 0.8;
//        }else {
//            rate = 1.0;
//        }
//
//        return rate;
//    }

    public static List<Integer> season1 = Arrays.asList(1,2,3);//第一季度
    public static List<Integer> season2 = Arrays.asList(4,5,6);//第二季度
    public static List<Integer> season3 = Arrays.asList(7,8,9);//第三季度
    public static List<Integer> season4 = Arrays.asList(10,11,12);//第三季度
    //传进入职日期，返回季中月15号
    public static Date MidSeasonDate(Date probation_date) throws Exception {
        Calendar calendar = Calendar.getInstance();
        //入职日期
        calendar.setTime(probation_date);
        int month = calendar.get(Calendar.MONTH) + 1;
        int year = calendar.get(Calendar.YEAR);
        if(season1.contains(month)){
            calendar.set(year, Calendar.FEBRUARY,15);
        }else if(season2.contains(month)){
            calendar.set(year, Calendar.MAY,15);
        }else if(season3.contains(month)){
            calendar.set(year, Calendar.AUGUST,15);
        }else if(season4.contains(month)){
            calendar.set(year, Calendar.NOVEMBER,15);
        }
        return calendar.getTime();
    }
    //判断参加几个月考核
    public static String AssessMonth(Date hire_date) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar calendar = Calendar.getInstance();//当前日期
        Calendar calendar1 = Calendar.getInstance();//当前日期的上个季度的季中月15日
        Calendar calendar2 = Calendar.getInstance();//当前日期的上个季度的季末月15日
        Calendar calendar3 = Calendar.getInstance();//当前日期的当前季度的季前月15日
        Calendar calendar4 = Calendar.getInstance();//当前日期的当前季度的季中月15日
        String assessMonth = "";
        calendar.setTime(sdf.parse(sdf.format(System.currentTimeMillis())));
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;
        //初始化判断条件
        if(season1.contains(month)){
            calendar1.set(year-1,Calendar.NOVEMBER,15);//11-15
            calendar2.set(year-1,Calendar.DECEMBER,15);//12-15
            calendar3.set(year,Calendar.JANUARY,15);//01-15
            calendar4.set(year,Calendar.FEBRUARY,15);//02-15
        }else if(season2.contains(month)){
            calendar1.set(year,Calendar.FEBRUARY,15);//02-15
            calendar2.set(year,Calendar.MARCH,15);//03-15
            calendar3.set(year,Calendar.APRIL,15);//04-15
            calendar4.set(year,Calendar.MAY,15);//05-15
        }else if(season3.contains(month)){
            calendar1.set(year,Calendar.MAY,15);//05-15
            calendar2.set(year,Calendar.JUNE,15);//06-15
            calendar3.set(year,Calendar.JULY,15);//07-15
            calendar4.set(year,Calendar.AUGUST,15);//08-15
        }else if(season4.contains(month)){
            calendar1.set(year,Calendar.AUGUST,15);//08-15
            calendar2.set(year,Calendar.SEPTEMBER,15);//09-15
            calendar3.set(year,Calendar.OCTOBER,15);//10-15
            calendar4.set(year,Calendar.NOVEMBER,15);//11-15
        }

        //判断参加几个月考核
        if( hire_date.before(calendar1.getTime()) || (hire_date.after(calendar2.getTime()) && hire_date.before(calendar3.getTime())) ){
            assessMonth = "3";
        }else if( hire_date.after(calendar1.getTime()) && hire_date.before(calendar2.getTime()) ){
            assessMonth = "4";
        }else if( hire_date.after(calendar3.getTime()) && hire_date.before(calendar4.getTime()) ){
            assessMonth = "2";
        }else {
            assessMonth = "0";
        }
        return assessMonth;
    }
    //是否是月初
    public static String IsFirstDay(Date now){
        Calendar calendar = Calendar.getInstance();//当前日期
        String isFirstDayFlag = "";
        calendar.setTime(now);
        if (1 == calendar.get(Calendar.DAY_OF_MONTH)) {
            isFirstDayFlag = "Y";
        }else {
            isFirstDayFlag = "N";
        }
        return isFirstDayFlag;
    }
    //获得月初第一天
    public static String getFirstDay(Date now){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();//当前日期
        calendar.setTime(now);
        calendar.set(Calendar.DAY_OF_MONTH,1);
        return sdf.format(calendar.getTime());
    }


    //是否是季初
    public static String IsFirstDayOfSeason(Date now){
        Calendar calendar = Calendar.getInstance();//当前日期
        String isFirstDayOfSeason = "";
        calendar.setTime(now);
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        int month = calendar.get(Calendar.MONTH) + 1;
        if(1 == day && ( 1 == month || 4 == month || 7 == month || 10 == month) ){
            isFirstDayOfSeason = "Y";
        }else {
            isFirstDayOfSeason = "N";
        }
        return isFirstDayOfSeason;
    }

    //传进机构号, 返回 机构号对应值
    public static String IsSpecialBranch(String branch_id, HTable hTable) throws Exception {
        String branchValue = null;
        Result branchResult = Util.getHbaseResultSync(branch_id, hTable);
        if (!branchResult.isEmpty()) {
            for (Cell listCell : branchResult.listCells()) {
                branchValue = Bytes.toString(CellUtil.cloneValue(listCell));
            }
        }
        return branchValue;
    }

    //考核维持标准
    public static String getKeepAssessmentGoal(String rank,String flag){
        String keepAssessmentGoal = "";
        double dKeepAssessmentGoal;
        dKeepAssessmentGoal = map.get(rank).f0;
        if("YES".equalsIgnoreCase(flag)){
            dKeepAssessmentGoal = dKeepAssessmentGoal * 1.2;
        }

        keepAssessmentGoal = String.valueOf(dKeepAssessmentGoal);
        return keepAssessmentGoal;
    }

    //考核结果
    public static List<String> rank1 = Arrays.asList("A01","A02","A03","A04","A05","A06","A07","A08","A09");//专员职级
    public static List<String> rank2 = Arrays.asList("A10","A11","A12","A13","A14");//部经理职级
    public static List<String> rank3 = Arrays.asList("A15","A16","A17","A18","A19");//区域总监职级
    public static List<Integer> firstMonth = Arrays.asList(1,4,7,10);//季度首月
    public static List<Integer> secondMonth = Arrays.asList(2,5,8,11);//季度中月
    public static List<Integer> thirdMonth = Arrays.asList(3,6,9,12);//季度末月
    public static String AssessResult(String sales_id, String rank, String s_flag, double factPrem,
                                      String assessMonth,String nowAssessRate,HTable hTable) throws Exception {
        Tuple3<Double, Double, Tuple2<Double, Double>> tuple3 = map.get(rank);
        String flag = "";
        if("0".equalsIgnoreCase(assessMonth)){
            //该代理人不参加考核,考核结果为 '-'
            flag = "-";
        }else {
            double parse = isDoubleNotNull(assessMonth);
            //判断标准
            double rate = 0.0;
            if (rank1.contains(rank)){
                if ("YES".equalsIgnoreCase(s_flag)){//是上海 浙江 宁波 分公司
                    if ("09".equalsIgnoreCase(rank)){
                        if(factPrem < (tuple3.f0 * 1.2 / 3 * parse)){
                            flag = "3";//降级
                        }else {
                            flag = "2";//维持
                        }
                    }else {
                        if(factPrem < (tuple3.f0 * 1.2 / 3 * parse)){
                            if ("01".equalsIgnoreCase(rank) && factPrem < (36000.0 / 3 * parse)){
                                flag = "4";//辞退
                            }else {
                                flag = "3";//降级
                            }
                        }else if( (tuple3.f0 * 1.2 / 3 * parse) <=  factPrem && factPrem < (tuple3.f1 * 1.2 / 3 * parse)){
                            flag = "2";//维持
                        }else if(factPrem >= (tuple3.f1 * 1.2 / 3 * parse)){
                            flag = "1";//晋级
                        }
                    }
                }
                else {//不是上海 浙江 宁波 分公司
                    if ("09".equalsIgnoreCase(rank)){
                        if(factPrem < (tuple3.f0 / 3 * parse)){
                            flag = "3";//降级
                        }else {
                            flag = "2";//维持
                        }
                    }else {
                        if(factPrem < (tuple3.f0 / 3 * parse)){
                            if ("01".equalsIgnoreCase(rank) && factPrem < (30000.0 / 3 * parse)){
                                flag = "4";//辞退
                            }else {
                                flag = "3";//降级
                            }
                        }else if((tuple3.f0 / 3 * parse) <=  factPrem && factPrem < (tuple3.f1 / 3 * parse)){
                            flag = "2";//维持
                        }else if(factPrem >= (tuple3.f1 / 3 * parse)){
                            flag = "1";//晋级
                        }
                    }
                }
            }

            //----------------------------------------------------------------------
            // 部经理 / 区域总监
            else if (rank2.contains(rank) || rank3.contains(rank)){
                rate = SunUtils.getHistoryRate(sales_id,assessMonth,rank,nowAssessRate,hTable);
                //判断考核结果
                if("YES".equalsIgnoreCase(s_flag)){//是 上海 浙江 宁波 分公司
                    if("A14".equalsIgnoreCase(rank) || "A19".equalsIgnoreCase(rank)){//是否是最高职级
                        if( ((tuple3.f0 * 1.2 / 3 * parse) <=  factPrem) && (tuple3.f2.f0 <= rate)){
                            flag = "2";//维持
                        }else {
                            flag = "3";//降级
                        }
                    }
                    else {//其他职级
                        if(factPrem >= (tuple3.f1 * 1.2 / 3 * parse) && rate >= tuple3.f2.f1){
                            flag = "1";//晋级
                        }else if( ( (tuple3.f0 * 1.2 / 3 * parse) <=  factPrem && factPrem < (tuple3.f1 * 1.2 / 3 * parse) )
                                && (rate > tuple3.f2.f0 && rate < tuple3.f2.f1)){
                            flag = "2";//维持
                        }else{
                            flag = "3";//降级
                        }
                    }
                }else {//不是 上海 浙江 宁波 分公司
                    if("A14".equalsIgnoreCase(rank) || "A19".equalsIgnoreCase(rank) ){//是否是最高职级
                        if( ((tuple3.f0 / 3 * parse) <=  factPrem) && (tuple3.f2.f0 <= rate)){
                            flag = "2";//维持
                        }else {
                            flag = "3";//降级
                        }
                    }else {
                        if(factPrem >= (tuple3.f1 / 3 * parse) && rate >= tuple3.f2.f1){
                            flag = "1";//晋级
                        }else if( ((tuple3.f0 / 3 * parse) <=  factPrem &&  (tuple3.f1 / 3 * parse) > factPrem)
                                && (rate > tuple3.f2.f0 && rate < tuple3.f2.f1)){
                            flag = "2";//维持
                        }else{
                            flag = "3";//降级
                        }
                    }
                }

            }
        }

        return flag;
    }

    //是否底薪全额获得
    public static String isFullSalary(String rank,String s_flag,double prem){
        String is_FullSalary = "";
        Tuple3<Double, Double, Tuple2<Double, Double>> tuple3 = map.get(rank);
        if ("YES".equalsIgnoreCase(s_flag)) {
            if(prem < (tuple3.f0 * 1.2 / 3)){
                is_FullSalary = "0";
            }else {
                is_FullSalary = "1";
            }
        }else {
            if(prem < (tuple3.f0 / 3)){
                is_FullSalary = "0";
            }else {
                is_FullSalary = "1";
            }
        }
        return is_FullSalary;
    }

    //是否挂零
    public static String isZero(double prem){
        String is_zero = "";
        if (prem <= 0.0){
            is_zero = "1";
        }else {
            is_zero = "0";
        }
        return is_zero;
    }

    //查询月末日期
    public static String getDate(Calendar c,Integer integer){
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
        //指定日期月份减去一
        c.add(Calendar.MONTH, integer);
        //指定日期月份减去一后的 最大天数
        c.set(Calendar.DATE, c.getActualMaximum(Calendar.DATE));
        //返回日期
        return sdf1.format(c.getTime());
    }

    //代理人考核后职级
    public static String toAgentGrade(double prem, String rank, String flag, String assessMonth){
        //将考核月份转为数字
        double parse = isDoubleNotNull(assessMonth);
        String toGrade = "";
        Set<Map.Entry<String, Tuple3<Double, Double, Tuple2<Double, Double>>>> entries = map.entrySet();
        for (Map.Entry<String, Tuple3<Double, Double, Tuple2<Double, Double>>> entry : entries) {
            String key = entry.getKey();
            Tuple3<Double, Double, Tuple2<Double, Double>> value = entry.getValue();
            if("YES".equalsIgnoreCase(flag)){//是 上海 / 浙江 / 宁波 分公司
                //专员
                if(rank1.contains(rank)){
                    if((30000.0 / 3  * parse * 1.2) > prem){
                        toGrade = "A00";
                    }else if((810000.0 / 3  * parse * 1.2) <= prem){
                        toGrade = "A09";
                    }else if((value.f0 / 3 * parse * 1.2) <= prem
                            && (value.f1 / 3 * parse * 1.2) > prem){
                        toGrade = key;
                    }
                }
                //部经理
                else if(rank2.contains(rank)){
                    if((900000.0 / 3 * parse * 1.2) > prem){
                        toGrade = "A10";
                    }else if((3000000.0 / 3 * parse * 1.2) <= prem){
                        toGrade = "A14";
                    }else if((value.f0 / 3 * parse * 1.2) <= prem
                            && (value.f1 / 3 * parse * 1.2) > prem){
                        toGrade = key;
                    }
                }
                //区域总监
                else if(rank3.contains(rank)){
                    if((3600000.0 / 3 * parse * 1.2) > prem){
                        toGrade = "A15";
                    }else if((6600000.0 / 3 * parse * 1.2) <= prem){
                        toGrade = "A19";
                    }else if((value.f0 / 3 * parse * 1.2) <= prem
                            && (value.f1 / 3 * parse * 1.2) > prem){
                        toGrade = key;
                    }
                }
            }else {//不是 上海 / 浙江 / 宁波 分公司
                //专员
                if(rank1.contains(rank)){
                    if((30000.0 / 3  * parse) > prem){
                        toGrade = "A00";
                    }else if((810000.0 / 3  * parse) <= prem){
                        toGrade = "A09";
                    }else if((value.f0 / 3 * parse) <= prem
                            && (value.f1 / 3 * parse) > prem){
                        toGrade = key;
                    }
                }
                //部经理
                else if(rank2.contains(rank)){
                    if((900000.0 / 3 * parse) > prem){
                        toGrade = "A10";
                    }else if((3000000.0 / 3 * parse) <= prem){
                        toGrade = "A14";
                    }else if((value.f0 / 3 * parse) <= prem
                            && (value.f1 / 3 * parse) > prem){
                        toGrade = key;
                    }
                }
                //区域总监
                else if(rank3.contains(rank)){
                    if((3600000.0 / 3 * parse) > prem){
                        toGrade = "A15";
                    }else if((6600000.0 / 3 * parse) <= prem){
                        toGrade = "A19";
                    }else if((value.f0 / 3 * parse) <= prem
                            && (value.f1 / 3 * parse) > prem){
                        toGrade = key;
                    }
                }
            }
        }
        return toGrade;
    }

    public static String actManpower(double oldPrem,double newPrem, String old_manpower,String flag){
        int newManpower = 0;//人力
        double standard = 10000.0;
        int oldManpower;
        if(StringUtils.isNotBlank(old_manpower)){
            oldManpower = Integer.parseInt(old_manpower);
        }else {
            oldManpower = 0;
        }
        if("YES".equalsIgnoreCase(flag)){
            standard = standard * 1.2;
        }
        if(oldPrem < standard && newPrem >= standard){
            newManpower = oldManpower + 1;
        }else if(oldPrem >= standard && newPrem < standard){
            newManpower = Math.max(0, oldManpower - 1);
        }
        return String.valueOf(newManpower);
    }

    //获取季度活动率
    public static Double getHistoryRate(String sales_id,String assessMonth,String rank,String nowAssessRate,HTable hTable) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar calendar = Calendar.getInstance();//当前日期
        double rate = 0;
        String rowKey = null;
        String date_id = "";//日期
        //考核月数
        int aMonth;
        if(StringUtils.isNotBlank(assessMonth)){
            aMonth = Integer.parseInt(assessMonth);
        }else {
            return null;
        }
        //当月活动率
        double aRate = isDoubleNotNull(nowAssessRate);
        calendar.setTime(sdf.parse(sdf.format(System.currentTimeMillis())));
        //当前月份
        int month = calendar.get(Calendar.MONTH) + 1;
        //当前月份在季度首月
        if(firstMonth.contains(month)){
            if(3 == aMonth){//考核月数为 3 个月
                rate = aRate;
            }
            else if(4 == aMonth){//考核月数为 4 个月
                double denominator = 1.0;//除数
                double rate1 = 0.0;
                //Todo 需要修改查询离线结果
                date_id = SunUtils.getDate(calendar,-1);
                rowKey = date_id + sales_id;
                Result result1 = Util.getHbaseResultSync(rowKey,hTable);
                if (!result1.isEmpty()) {
                    for (Cell listCell : result1.listCells()) {
                        JSONObject jsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                        if(rank2.contains(rank)){
                            rate1 = isDoubleNotNull(jsonObject.getString("department_activity_rate_m"));
                        }else {
                            rate1 = isDoubleNotNull(jsonObject.getString("distinct_activity_rate_m"));
                        }
                    }
                    denominator = denominator + 1.0;
                }
                //保留两位小数
                rate = isDoubleNotNull(String.format("%.2f",(aRate + rate1) / denominator));
            }
        }
        //当前月份在季度中月
        else if(secondMonth.contains(month)){
            if(2 == aMonth){//考核月数为 2 个月
                rate = aRate;
            }
            else if(3 == aMonth){//考核月数为 3 个月
                double denominator = 1.0;//除数
                double rate1 = 0.0;
                //Todo 需要修改查询离线结果
                date_id = SunUtils.getDate(calendar,-1);
                rowKey = date_id + sales_id;
                Result result1 = Util.getHbaseResultSync(rowKey,hTable);
                if (!result1.isEmpty()) {
                    for (Cell listCell : result1.listCells()) {
                        JSONObject jsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                        if(rank2.contains(rank)){
                            rate1 = isDoubleNotNull(jsonObject.getString("department_activity_rate_m"));
                        }else {
                            rate1 = isDoubleNotNull(jsonObject.getString("distinct_activity_rate_m"));
                        }
                    }
                    denominator = denominator + 1.0;
                }
                rate = (double) Math.round((aRate + rate1) / denominator);
            }
            else if(4 == aMonth){//考核月数为 4 个月
                double denominator = 1.0;//除数
                double rate1 = 0.0;//上个月率值
                double rate2 = 0.0;//上上个月率值
                //Todo 需要修改查询离线结果
                date_id = SunUtils.getDate(calendar,-1);
                rowKey = date_id + sales_id;
                Result result1 = Util.getHbaseResultSync(rowKey,hTable);
                if (!result1.isEmpty()) {
                    for (Cell listCell : result1.listCells()) {
                        JSONObject jsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                        if(rank2.contains(rank)){
                            rate1 = isDoubleNotNull(jsonObject.getString("department_activity_rate_m"));
                        }else {
                            rate1 = isDoubleNotNull(jsonObject.getString("distinct_activity_rate_m"));
                        }
                    }
                    denominator = denominator + 1.0;
                }
                //Todo 需要修改查询离线结果
                date_id = SunUtils.getDate(calendar,-2);
                rowKey = date_id + sales_id;
                Result result2 = Util.getHbaseResultSync(rowKey,hTable);
                if (!result2.isEmpty()) {
                    for (Cell listCell : result2.listCells()) {
                        JSONObject jsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                        if(rank2.contains(rank)){
                            rate1 = isDoubleNotNull(jsonObject.getString("department_activity_rate_m"));
                        }else {
                            rate1 = isDoubleNotNull(jsonObject.getString("distinct_activity_rate_m"));
                        }
                    }
                    denominator = denominator + 1.0;
                }
                rate = (double) Math.round((aRate + rate1 + rate2) / denominator);
            }
        }
        else if(thirdMonth.contains(month)){//当前月份在季度末月
            //考核月数为 2 个月
            if(2 == aMonth){
                double denominator = 1.0;//除数
                double rate1 = 0.0;//上月率值
                //Todo 需要修改查询离线结果
                date_id = SunUtils.getDate(calendar,-1);
                rowKey = date_id + sales_id;
                Result result1 = Util.getHbaseResultSync(rowKey,hTable);
                if (!result1.isEmpty()) {
                    for (Cell listCell : result1.listCells()) {
                        JSONObject jsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                        if(rank2.contains(rank)){
                            rate1 = isDoubleNotNull(jsonObject.getString("department_activity_rate_m"));
                        }else {
                            rate1 = isDoubleNotNull(jsonObject.getString("distinct_activity_rate_m"));
                        }
                    }
                    denominator = denominator + 1.0;
                }
                rate = (double) Math.round((aRate + rate1) / denominator);
            }
            else if(3 == aMonth){//考核月数为 3 个月
                double denominator = 1.0;//除数
                double rate1 = 0.0;//上月率值
                //Todo 需要修改查询离线结果
                date_id = SunUtils.getDate(calendar,-1);
                rowKey = date_id + sales_id;
                Result result1 = Util.getHbaseResultSync(rowKey,hTable);
                if (!result1.isEmpty()) {
                    for (Cell listCell : result1.listCells()) {
                        JSONObject jsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                        if(rank2.contains(rank)){
                            rate1 = isDoubleNotNull(jsonObject.getString("department_activity_rate_m"));
                        }else {
                            rate1 = isDoubleNotNull(jsonObject.getString("distinct_activity_rate_m"));
                        }
                    }
                    denominator = denominator + 1;
                }
                double rate2 = 0.0;//上上月率值
                //Todo 需要修改查询离线结果
                date_id = SunUtils.getDate(calendar,-2);
                rowKey = date_id + sales_id;
                Result result2 = Util.getHbaseResultSync(rowKey,hTable);
                if (!result2.isEmpty()) {
                    for (Cell listCell : result2.listCells()) {
                        JSONObject jsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                        if(rank2.contains(rank)){
                            rate2 = isDoubleNotNull(jsonObject.getString("department_activity_rate_m"));
                        }else {
                            rate2 = isDoubleNotNull(jsonObject.getString("distinct_activity_rate_m"));
                        }
                    }
                    denominator = denominator + 1;
                }
                rate = (double) Math.round((aRate + rate1 + rate2) / denominator);
            }
            //考核月数为 4 个月
            else if(4 == aMonth){
                double denominator = 1.0;//除数
                double rate1 = 0.0;//上月率值
                //Todo 需要修改查询离线结果
                date_id = SunUtils.getDate(calendar,-1);
                rowKey = date_id + sales_id;
                Result result1 = Util.getHbaseResultSync(rowKey,hTable);
                if (!result1.isEmpty()) {
                    for (Cell listCell : result1.listCells()) {
                        JSONObject jsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                        if(rank2.contains(rank)){
                            rate1 = isDoubleNotNull(jsonObject.getString("department_activity_rate_m"));
                        }else {
                            rate1 = isDoubleNotNull(jsonObject.getString("distinct_activity_rate_m"));
                        }
                    }
                    denominator = denominator + 1;
                }
                double rate2 = 0.0;//上上月率值
                //Todo 需要修改查询离线结果
                date_id = SunUtils.getDate(calendar,-2);
                rowKey = date_id + sales_id;
                Result result2 = Util.getHbaseResultSync(rowKey,hTable);
                if (!result2.isEmpty()) {
                    for (Cell listCell : result2.listCells()) {
                        JSONObject jsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                        if(rank2.contains(rank)){
                            rate2 = isDoubleNotNull(jsonObject.getString("department_activity_rate_m"));
                        }else {
                            rate2 = isDoubleNotNull(jsonObject.getString("distinct_activity_rate_m"));
                        }
                    }
                    denominator = denominator + 1;
                }
                double rate3 = 0.0;//上上上月率值
                //Todo 需要修改查询离线结果
                date_id = SunUtils.getDate(calendar,-3);
                rowKey = date_id + sales_id;
                Result result3 = Util.getHbaseResultSync(rowKey,hTable);
                if (!result3.isEmpty()) {
                    for (Cell listCell : result3.listCells()) {
                        JSONObject jsonObject = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)));
                        if(rank2.contains(rank)){
                            rate3 = isDoubleNotNull(jsonObject.getString("department_activity_rate_m"));
                        }else {
                            rate3 = isDoubleNotNull(jsonObject.getString("distinct_activity_rate_m"));
                        }
                    }
                    denominator = denominator + 1;
                }
                rate = (double) Math.round((aRate + rate1 + rate2 + rate3) / denominator);
            }
        }
        return rate;
    }

    public static double isDoubleNotNull(String value){
        double result;
        if(StringUtils.isNotBlank(value)){
            result = Double.parseDouble(value);
        }else {
            result = 0.0;
        }
        return result;
    }
}