package com.pactera.yhl.apps.measure.aggregate;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.measure.SunUtils;
import com.pactera.yhl.apps.measure.entity.FactWageBase;
import com.pactera.yhl.apps.measure.entity.Measure2;
import com.pactera.yhl.sink.abstr.MyHbaseCli;
import com.pactera.yhl.util.Util;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class AssessMap extends RichMapFunction<Tuple2<String, Measure2>,FactWageBase>{
    private static final List<String> rank1 = Arrays.asList("A01","A02","A03","A04","A05","A06","A07","A08","A09");//专员职级
    private static final List<String> rank2 = Arrays.asList("A10","A11","A12","A13","A14");//部经理职级
    private static final List<String> rank3 = Arrays.asList("A15","A16","A17","A18","A19");//区域总监职级

    private static final List<Integer> season1 = Arrays.asList(1,2,3);//第一季度
    private static final List<Integer> season2 = Arrays.asList(4,5,6);//第二季度
    private static final List<Integer> season3 = Arrays.asList(7,8,9);//第三季度
    private static final List<Integer> season4 = Arrays.asList(10,11,12);//第三季度

    public static List<Integer> firstMonth = Arrays.asList(1,4,7,10);//季度首月
    public static List<Integer> secondMonth = Arrays.asList(2,5,8,11);//季度中月
    public static List<Integer> thirdMonth = Arrays.asList(3,6,9,12);//季度末月

    public static Connection connection;
    public static HTable historyTable;

    MapState<String, FactWageBase> assessState;//中间状态对象
//    MapState<Integer,Integer> clearState;//触发清空状态对象

    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String config_path = params.get("config_path");
        connection = MyHbaseCli.hbaseConnection(config_path);
        try{
            //Todo 查询 APPLICATION_ASSESSMENT_PERSION_RESULT 表
            historyTable = (HTable) connection.getTable(TableName.valueOf("kl_base:application_assessment_persion_result"));
        }catch (Exception e){
            e.printStackTrace();
        }
//        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).cleanupIncrementally(1,false).build();
        assessState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("assessState",String.class,FactWageBase.class));

//        clearState = getRuntimeContext().getMapState(
//                new MapStateDescriptor<>("clearState",Integer.class,Integer.class));
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public FactWageBase map(Tuple2<String, Measure2> value) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM");

        Calendar calendar = Calendar.getInstance();
        //初始对象
        FactWageBase factWageBase = new FactWageBase();
        //进入值
        String sales_id = value.f0;
        Measure2 newMeasure = value.f1;
        //当前时间
        Date now = sdf.parse(sdf.format(System.currentTimeMillis()));
        calendar.setTime(sdf.parse(sdf.format(System.currentTimeMillis())));//yyyy-MM-dd HH:mm:ss
        //前一天
        calendar.add(Calendar.DAY_OF_MONTH,-1);

        int month = calendar.get(Calendar.MONTH) + 1;//当前月份

        //是否是变动数据标识
//        String is_change = newMeasure.getIs_change();

        //判断是否为首次进入计算
        if(null == assessState.get(sales_id)){
            //Todo 需要修改为查询数据接口
            String rowKey = sdf1.format(calendar.getTime()) + sales_id;
            //如果首次进入查询离线数据
            Result result = Util.getHbaseResultSync(rowKey,historyTable);
            if (!result.isEmpty()) {
                for (Cell listCell : result.listCells()) {
                    factWageBase = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell)),FactWageBase.class);
                    // 日 指标清空
                    factWageBase.setD_fyp("");
                    factWageBase.setDepartment_d_fyp("");
                    factWageBase.setDistinct_d_fyp("");
                    if("Y".equalsIgnoreCase(SunUtils.IsFirstDay(now))){
                        // 如果月初 , 月 指标清空
                        factWageBase.setM_fyp("");

                        factWageBase.setDepartment_m_fyp("");
                        factWageBase.setDepartment_activity_m("");
                        factWageBase.setDepartment_activity_rate_m("");

                        factWageBase.setDistinct_m_fyp("");
                        factWageBase.setDistinct_activity_m("");
                        factWageBase.setDistinct_activity_rate_m("");
                    }
                    if("Y".equalsIgnoreCase(SunUtils.IsFirstDayOfSeason(now))){
                        // 如果季初 , 季 指标清空
                        factWageBase.setQ_fyp("");

                        factWageBase.setDepartment_q_fyp("");
                        factWageBase.setDepartment_activity_q("");
                        factWageBase.setDepartment_activity_rate_q("");

                        factWageBase.setDistinct_q_fyp("");
                        factWageBase.setDistinct_activity_q("");
                        factWageBase.setDistinct_activity_rate_q("");
                    }
                }
            }
        }else {
            //否则查询中间状态数据
            factWageBase = assessState.get(sales_id);
        }

        // 统计日期
        factWageBase.setDay_id(sdf1.format(now));
        // 月份
        factWageBase.setMonth_id(String.valueOf(month));
        // 季度
        if(season1.contains(month)){
            factWageBase.setQuarterly_id("1");
        }else if(season2.contains(month)){
            factWageBase.setQuarterly_id("2");
        }else if(season3.contains(month)){
            factWageBase.setQuarterly_id("3");
        }else if(season4.contains(month)){
            factWageBase.setQuarterly_id("4");
        }
        // 主键
        factWageBase.setKey_id(sdf1.format(now) + "#" + sales_id);

        //获取职级
        String rank = newMeasure.getRank();
        //是否是特殊公司标识
        String flag = newMeasure.getFlag();

        // 作业地区
        factWageBase.setWorkarea(newMeasure.getWorkarea());
        // 入司日期
        factWageBase.setHire_date(sdf1.format(sdf.parse(newMeasure.getHire_date())));
        // 入司年月
        factWageBase.setHire_month(sdf2.format(sdf.parse(newMeasure.getHire_date())));
        // 代理人编码
        factWageBase.setAgent_id(newMeasure.getSales_id());
        // 代理人姓名
        factWageBase.setAgent_name(newMeasure.getSales_name());
        // 代理人状态
        factWageBase.setAgentstate(newMeasure.getStat());
        // 代理人职级
        factWageBase.setAgent_grade(rank);

        //---------------------------------------------------------------------------------
        // 部代码
        factWageBase.setDepartment_code(newMeasure.getDepartment_code());
        // 部名称
        factWageBase.setDepartment_name(newMeasure.getDepartment_name());
        // 部经理代码
        factWageBase.setDepartment_agentcode(newMeasure.getDepartment_agentcode());
        // 部经理名称
        factWageBase.setDepartment_leader(newMeasure.getDepartment_leader());
        // 区代码
        factWageBase.setDistrict_code(newMeasure.getDistrict_code());
        // 区名称
        factWageBase.setDistrict_name(newMeasure.getDistrict_name());
        // 总监代码
        factWageBase.setDistrict_agentcode(newMeasure.getDistrict_agentcode());
        // 总监名称
        factWageBase.setDistrict_leader(newMeasure.getDistrict_leader());

        //---------------------------------------------------------------------------------
        String provincecom_code = newMeasure.getProvincecom_code();
        if("920000".equalsIgnoreCase(provincecom_code)){
            // 分公司代码
            factWageBase.setProvincecom_code("8633");
            // 分公司名称
            factWageBase.setProvincecom_name("浙江分公司");
        }else {
            // 分公司代码
            factWageBase.setProvincecom_code("86" + provincecom_code.substring(0,2));
            // 分公司名称
            factWageBase.setProvincecom_name(newMeasure.getProvincecom_name());
        }

        //---------------------------------------------------------------------------------
        //旧个人月度标准保费
        double oldM_fyp = SunUtils.isDoubleNotNull(factWageBase.getM_fyp());
        //新个人季度标准保费
        double oldQ_fyp = SunUtils.isDoubleNotNull(factWageBase.getQ_fyp());

        //新进标保
        double newSlf_prem = newMeasure.getSlf_prem();//个人
        double newTeam_prem = newMeasure.getTeam_prem();//团队

        //---------------------------------------------------------------------------------
        // 个人日度标准保费
        factWageBase.setD_fyp(String.valueOf(SunUtils.isDoubleNotNull(factWageBase.getD_fyp()) + newSlf_prem));
        // 个人月度标准保费
        factWageBase.setM_fyp(String.valueOf(oldM_fyp + newSlf_prem));
        // 个人季度标准保费
        factWageBase.setQ_fyp(String.valueOf(oldQ_fyp + newSlf_prem));


        //---------------------------------------------------------------------------------
        //考核月份
        factWageBase.setAssessmonth(SunUtils.AssessMonth(sdf1.parse(newMeasure.getHire_date())));

        //新个人月度标准保费
        double newM_fyp = SunUtils.isDoubleNotNull(factWageBase.getM_fyp());
        //新个人季度标准保费
        double newQ_fyp = SunUtils.isDoubleNotNull(factWageBase.getQ_fyp());
        // 是否达到活动人力
        double isManpowerStandard;
        if("YES".equalsIgnoreCase(flag)){
            isManpowerStandard = 10000.0 * 1.2;
        }else {
            isManpowerStandard = 10000.0;
        }
        if(newM_fyp >= isManpowerStandard){
            factWageBase.setIs_manpower("YES");
        }else {
            factWageBase.setIs_manpower("NO");
        }
        // 个人继续率
        factWageBase.setMember_rolling_rate("1");

        //---------------------------------------------------------------------------------
        // 季度所在月数
        if (firstMonth.contains(month)){
            factWageBase.setMonth_num("1");
        }else if (secondMonth.contains(month)){
            factWageBase.setMonth_num("2");
        }else if (thirdMonth.contains(month)){
            factWageBase.setMonth_num("3");
        }
        //考核维持标准
        factWageBase.setAssessment_standard(SunUtils.getKeepAssessmentGoal(rank,flag));

        //专员
        if (rank1.contains(rank)) {
            //目标差值
            BigDecimal bigDecimal = BigDecimal.valueOf(SunUtils.isDoubleNotNull(factWageBase.getAssessment_standard()) -
                    SunUtils.isDoubleNotNull(factWageBase.getQ_fyp()));
            factWageBase.setAssessment_difference(String.valueOf(bigDecimal.setScale(2, RoundingMode.HALF_UP).doubleValue()));

            //是否底薪全额获得
            factWageBase.setIs_fullsalary(SunUtils.isFullSalary(rank, flag, SunUtils.isDoubleNotNull(factWageBase.getQ_fyp())));

            //考核结果
            factWageBase.setAssessment_result(SunUtils.AssessResult("", rank, flag,
                    SunUtils.isDoubleNotNull(factWageBase.getQ_fyp()), factWageBase.getAssessmonth(),"",historyTable));

            //代理人考核后职级
            if("0".equalsIgnoreCase(factWageBase.getAssessment_result())){
                //如果该代理人不参加考核,则考核前职级为当前职级
                factWageBase.setTo_agent_grade(rank);
            }else {
                //如果该代理人不参加考核,则考核后职级为当前职级
                //如果此专员考核结果是降级,取上个季度最后一天的季度标保
                // 连续两个季度的标保都在3W-9W之间,则该专员考核后职级为 辞退 A00
                if("3".equalsIgnoreCase(factWageBase.getAssessment_result())){
                    double nowPrem = SunUtils.isDoubleNotNull(factWageBase.getQ_fyp());//当季度标保
                    int year = calendar.get(Calendar.YEAR);
                    if (season1.contains(month)) {
                        calendar.set(year - 1, Calendar.DECEMBER,31);//12-31
                    }else if (season2.contains(month)) {
                        calendar.set(year, Calendar.MARCH,31);//03-31
                    }else if (season3.contains(month)) {
                        calendar.set(year, Calendar.JUNE,30);//06-30
                    }else if (season4.contains(month)) {
                        calendar.set(year, Calendar.SEPTEMBER,30);//09-30
                    }
                    String rowKey = sdf1.format(calendar.getTime())  + sales_id;
                    //Todo 需要查询离线数据
                    Result preResult = Util.getHbaseResultSync(rowKey,historyTable);
                    if (!preResult.isEmpty()) {
                        for (Cell listCell : preResult.listCells()) {
                            String q_fyp = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell))).getString("q_fyp");
                            double oldPrem = SunUtils.isDoubleNotNull(q_fyp);//上季度标保
                            if("YES".equalsIgnoreCase(flag)){//是 上海 / 浙江 / 宁波 分公司
                                if((36000.0 <= oldPrem && oldPrem < 108000.0) &&
                                        (36000.0 <= nowPrem && nowPrem < 108000.0)){
                                    factWageBase.setTo_agent_grade("A00");
                                }
                            }else {//不是 上海 / 浙江 / 宁波 分公司
                                if((30000.0 <= oldPrem && oldPrem < 90000.0) &&
                                        (30000.0 <= nowPrem && nowPrem < 90000.0)){
                                    factWageBase.setTo_agent_grade("A00");
                                }
                            }
                        }
                    }else {
                        //如果为查询到离线数据则取最低代理人职级
                        factWageBase.setTo_agent_grade("A01");
                    }
                }else {
                    factWageBase.setTo_agent_grade(SunUtils.toAgentGrade(
                            SunUtils.isDoubleNotNull(factWageBase.getQ_fyp()),rank,
                            flag,factWageBase.getAssessmonth()));
                }
            }

            //是否挂零
            factWageBase.setIs_zero(SunUtils.isZero(SunUtils.isDoubleNotNull(factWageBase.getQ_fyp())));
        }
        // 部经理
        else if (rank2.contains(rank)) {
            // 所辖部月度标准保费
            factWageBase.setDepartment_m_fyp(String.valueOf(
                    SunUtils.isDoubleNotNull(factWageBase.getDepartment_m_fyp()) + newTeam_prem));
            // 时点所辖部季度考核人力
            factWageBase.setDepartment_assessment(String.valueOf(newMeasure.getLabor()));

            //Todo 所辖部月度考核人力
            if("Y".equalsIgnoreCase(SunUtils.IsFirstDay(now))){
                factWageBase.setDepartment_assessment_m(factWageBase.getDepartment_assessment());
            }else {
                // 时点所辖部月度考核人力
                double newAssess = SunUtils.isDoubleNotNull(factWageBase.getDepartment_assessment());
                // 月初所辖部月度考核人力 , 查询该 代理人 月初信息
                String rowKey = SunUtils.getFirstDay(calendar.getTime()) + sales_id;
                double oldAssess = 0.0;
                Result historyResult = Util.getHbaseResultSync(rowKey, historyTable);
                if(!historyResult.isEmpty()){
                    for (Cell listCell : historyResult.listCells()) {
                        String department_assessment = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell))).getString("department_assessment");
                        if (StringUtils.isNotBlank(department_assessment)){
                            oldAssess = SunUtils.isDoubleNotNull(department_assessment);
                        }
                    }
                }else {
                    //如果为空则查询 代理人 入职当天信息
                    rowKey = newMeasure.getHire_date().substring(0,10) + sales_id;
                    Result historyResult1 = Util.getHbaseResultSync(rowKey,historyTable);
                    if (historyResult1.isEmpty()) {
                        for (Cell listCell : historyResult1.listCells()) {
                            String department_assessment = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell))).getString("department_assessment");
                            if (StringUtils.isNotBlank(department_assessment)){
                                oldAssess = SunUtils.isDoubleNotNull(department_assessment);
                            }
                        }
                    }
                }
                //向上取整
                int result = (int) Math.ceil((newAssess + oldAssess) / 2);
                factWageBase.setDepartment_assessment_m(String.valueOf(result));
            }


            // 所辖部季度标准保费
            factWageBase.setDepartment_q_fyp(String.valueOf(
                    SunUtils.isDoubleNotNull(factWageBase.getDepartment_q_fyp()) + newTeam_prem));

            //所辖部月度活动人力
            factWageBase.setDepartment_activity_m(
                    SunUtils.actManpower(oldM_fyp,newM_fyp,factWageBase.getDepartment_activity_m(),flag));
            //所辖部季度活动人力
            factWageBase.setDepartment_activity_q(
                    SunUtils.actManpower(oldQ_fyp,newQ_fyp,factWageBase.getDepartment_activity_q(),flag));

            //所辖部月度人员活动率
            double department_assessment = SunUtils.isDoubleNotNull(factWageBase.getDepartment_assessment());
            if(department_assessment != 0.0){
                factWageBase.setDepartment_activity_rate_m(String.format(
                        "%.2f",SunUtils.isDoubleNotNull(factWageBase.getDepartment_activity_m()) / department_assessment));
            }else {
                factWageBase.setDepartment_activity_rate_m("0");
            }

            //Todo 所辖部季度人员活动率
            factWageBase.setDepartment_activity_rate_q(String.valueOf(SunUtils.getHistoryRate(
                    sales_id,factWageBase.getAssessmonth(),rank,factWageBase.getDepartment_activity_rate_m(),historyTable)));

            //---------------------------------------------------------------------------------
            //是否底薪全额获得
            factWageBase.setIs_fullsalary(SunUtils.isFullSalary(rank, flag,
                    SunUtils.isDoubleNotNull(factWageBase.getDepartment_m_fyp())));

            //目标差值
            BigDecimal bigDecimal = BigDecimal.valueOf(SunUtils.isDoubleNotNull(factWageBase.getAssessment_standard()) -
                    SunUtils.isDoubleNotNull(factWageBase.getDepartment_q_fyp()));
            factWageBase.setAssessment_difference(String.valueOf(bigDecimal.setScale(2, RoundingMode.HALF_UP).doubleValue()));

            //考核结果
            factWageBase.setAssessment_result(SunUtils.AssessResult(sales_id, rank, flag,
                    SunUtils.isDoubleNotNull(factWageBase.getDepartment_q_fyp()),
                    factWageBase.getAssessmonth(),factWageBase.getDepartment_activity_rate_m(),historyTable));

            //代理人考核后职级
            if("0".equalsIgnoreCase(factWageBase.getAssessment_result())){
                //如果该代理人不参加考核,则考核后职级为当前职级
                factWageBase.setTo_agent_grade(rank);
            }else {
                factWageBase.setTo_agent_grade(SunUtils.toAgentGrade(
                        SunUtils.isDoubleNotNull(factWageBase.getDepartment_q_fyp()),rank,
                        flag,factWageBase.getAssessmonth()));
            }

            //是否挂零
            factWageBase.setIs_zero(
                    SunUtils.isZero(SunUtils.isDoubleNotNull(factWageBase.getDepartment_q_fyp())));
            // 所辖部继续率
            factWageBase.setDepartment_rolling_rate("1");
        }

        // 区域总监
        else if (rank3.contains(rank)) {
            // 所辖区日标准保费
            factWageBase.setDistinct_d_fyp(String.valueOf(SunUtils.isDoubleNotNull(factWageBase.getDistinct_d_fyp()) + newTeam_prem));
            // 所辖区月度标准保费
            factWageBase.setDistinct_m_fyp(String.valueOf(SunUtils.isDoubleNotNull(factWageBase.getDistinct_m_fyp()) + newTeam_prem));
            // 所辖区月度考核人力
            factWageBase.setDistinct_assessment(String.valueOf(newMeasure.getLabor()));

            //Todo 所辖区月度考核人力
            if("Y".equalsIgnoreCase(SunUtils.IsFirstDay(now))){
                factWageBase.setDistinct_assessment_m(factWageBase.getDistinct_assessment());
            }else {
                // 时点所辖部月度考核人力
                double newAssess = SunUtils.isDoubleNotNull(factWageBase.getDepartment_assessment());
                // 月初所辖部月度考核人力
                String rowKey = SunUtils.getFirstDay(calendar.getTime()) + sales_id;

                double oldAssess = 0.0;
                Result historyResult = Util.getHbaseResultSync(rowKey, historyTable);
                if(!historyResult.isEmpty()){
                    for (Cell listCell : historyResult.listCells()) {
                        String distinct_assessment = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell))).getString("distinct_assessment");
                        if (StringUtils.isNotBlank(distinct_assessment)){
                            oldAssess = SunUtils.isDoubleNotNull(distinct_assessment);
                        }
                    }
                }else {
                    //如果为空则查询 代理人 入职当天信息
                    rowKey = newMeasure.getHire_date().substring(0,10) + sales_id;
                    Result historyResult1 = Util.getHbaseResultSync(rowKey,historyTable);
                    if (!historyResult1.isEmpty()) {
                        for (Cell listCell : historyResult1.listCells()) {
                            String distinct_assessment = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(listCell))).getString("distinct_assessment");
                            if (StringUtils.isNotBlank(distinct_assessment)){
                                oldAssess = SunUtils.isDoubleNotNull(distinct_assessment);
                            }
                        }
                    }
                }
                //向上取整
                int result = (int) Math.ceil((newAssess + oldAssess) / 2);
                factWageBase.setDistinct_assessment_m(String.valueOf(result));
            }

            // 所辖区季度标准保费
            factWageBase.setDistinct_q_fyp(String.valueOf(
                    SunUtils.isDoubleNotNull(factWageBase.getDistinct_q_fyp()) + newTeam_prem));

            //所辖区月度活动人力
            factWageBase.setDistinct_activity_m(
                    SunUtils.actManpower(oldM_fyp,newM_fyp,factWageBase.getDistinct_activity_m(),flag));
            //所辖区季度活动人力
            factWageBase.setDistinct_activity_q(
                    SunUtils.actManpower(oldQ_fyp,newQ_fyp,factWageBase.getDistinct_activity_q(),flag));

            //所辖区月度人员活动率
            double distinct_assessment = SunUtils.isDoubleNotNull(factWageBase.getDistinct_assessment());
            if(distinct_assessment != 0.0){
                factWageBase.setDistinct_activity_rate_m(String.format(
                        "%.2f",SunUtils.isDoubleNotNull(factWageBase.getDistinct_activity_m()) / distinct_assessment));
            }else {
                factWageBase.setDistinct_activity_m("0");
            }


            //Todo 所辖区季度人员活动率
            factWageBase.setDistinct_activity_rate_q(String.valueOf(SunUtils.getHistoryRate(
                    sales_id,factWageBase.getAssessmonth(),rank,factWageBase.getDistinct_activity_rate_m(),historyTable)));

            //---------------------------------------------------------------------------------
            //是否底薪全额获得
            factWageBase.setIs_fullsalary(SunUtils.isFullSalary(rank, flag,
                    SunUtils.isDoubleNotNull(factWageBase.getDistinct_m_fyp())));

            //目标差值
            BigDecimal bigDecimal = BigDecimal.valueOf(SunUtils.isDoubleNotNull(factWageBase.getAssessment_standard()) -
                    SunUtils.isDoubleNotNull(factWageBase.getDistinct_q_fyp()));
            factWageBase.setAssessment_difference(String.valueOf(bigDecimal.setScale(2, RoundingMode.HALF_UP).doubleValue()));

            //考核结果
            factWageBase.setAssessment_result(SunUtils.AssessResult(sales_id, rank, flag,
                    SunUtils.isDoubleNotNull(factWageBase.getDistinct_q_fyp()),
                    factWageBase.getAssessmonth(), factWageBase.getDistinct_activity_rate_m(),historyTable));

            //代理人考核后职级
            factWageBase.setTo_agent_grade(SunUtils.toAgentGrade(
                    SunUtils.isDoubleNotNull(factWageBase.getDistinct_q_fyp()),rank,
                    flag,factWageBase.getAssessmonth()));


            //是否挂零
            factWageBase.setIs_zero(
                    SunUtils.isZero(SunUtils.isDoubleNotNull(factWageBase.getDistinct_q_fyp())));
            // 所辖区继续率
            factWageBase.setDistinct_rolling_rate("1");
        }

        //---------------------------------------------------------------------------------
        //更新处理时间
        factWageBase.setLoad_date(sdf.format(System.currentTimeMillis()));

        //更新中间状态
        assessState.put(sales_id,factWageBase);

        //返回值
        return factWageBase;
    }

}