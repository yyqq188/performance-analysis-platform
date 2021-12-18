package com.pactera.yhl.apps.warning.map;

import com.pactera.yhl.apps.warning.entity.AssessmentResult;
import com.pactera.yhl.apps.warning.entity.FactWageBase;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author SUN KI
 * @time 2021/11/16 14:48
 * @Desc 薪资考核计算类
 */
public class CalKhxzProcessFunction extends RichMapFunction<FactWageBase, AssessmentResult> {
    //1.准备一个MapState用来存放上一个窗口的Fact_Prem数据
    //MapState<"totalNum",上一个窗口的总人数>
    private MapState<String, Long> khxzFState = null;//全额获得
    private MapState<String, Long> khxzZState = null;//挂零
    private MapState<String, Long> khxzTState = null;//总
    private static List<String> agentCodeSList = new ArrayList<>();//全局变量存agentcode

    //2.初始化State
    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, Long> khxzFStateDescriptor = new MapStateDescriptor<>("khxzFState", String.class, Long.class);
        MapStateDescriptor<String, Long> khxzZStateDescriptor = new MapStateDescriptor<>("khxzZState", String.class, Long.class);
        MapStateDescriptor<String, Long> khxzTStateDescriptor = new MapStateDescriptor<>("khxzTState", String.class, Long.class);
        khxzFState = getRuntimeContext().getMapState(khxzFStateDescriptor);
        khxzZState = getRuntimeContext().getMapState(khxzZStateDescriptor);
        khxzTState = getRuntimeContext().getMapState(khxzTStateDescriptor);
    }


    @Override
    public AssessmentResult map(FactWageBase newFactwagebase) throws Exception {

        long l = System.currentTimeMillis();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentTime = formatter.format(l);
        String khxzFstateDate;
        String key = null;
        for (Map.Entry<String, Long> entry : khxzFState.entries()) {
            if (entry.getKey() != null) {
                key = entry.getKey();
            }
        }
        if (key != null) {
            khxzFstateDate = key.substring(0, 2);
        } else {
            khxzFstateDate = "32";
        }

        //每天零点清理状态
        if (!khxzFstateDate.equals(currentTime.substring(8, 10))) {
            if (khxzFState != null) {
                khxzFState.clear();
            }
            if (khxzZState != null) {
                khxzZState.clear();
            }
            if (khxzTState != null) {
                khxzTState.clear();
            }
            if (agentCodeSList != null) {
                agentCodeSList.clear();
            }
        }

        //判断之前有没有这个代理人进来, 如果没进来过, 进行下面计算
        if (agentCodeSList == null || !agentCodeSList.contains(newFactwagebase.getAgent_id())) {
            //获取状态中上一窗口的pojo类
            Long flastNum = khxzFState.get(currentTime.substring(8, 10) + "fullSalaryNumber");
            Long zlastNum = khxzZState.get(currentTime.substring(8, 10) + "zeroSalaryNumber");
            Long tlastNum = khxzTState.get(currentTime.substring(8, 10) + "totalNumber");
            Long fnewNum;//各分区月全额人力
            Long znewNum;//各分区月挂零人力
            Long tnewNum;//各分区月考核总人力
            Double fmrm = 0D;//各分区月全额率
            Double zmrm = 0D;//各分区月挂零率
            //进来的是全额获得人力
            if ("1".equals(newFactwagebase.getIs_fullsalary())) {
                if (flastNum == null) {
                    fnewNum = 1L;
                } else {
                    fnewNum = flastNum + 1L;
                }
                if (zlastNum == null) {
                    znewNum = 0L;
                } else {
                    znewNum = zlastNum;
                }

            } else {
                //进来的是挂零人力
                if ("1".equals(newFactwagebase.getIs_zero())) {
                    if (flastNum == null) {
                        fnewNum = 0L;
                    } else {
                        fnewNum = flastNum;
                    }
                    if (zlastNum == null) {
                        znewNum = 1L;
                    } else {
                        znewNum = zlastNum + 1L;
                    }
                } else { //进来的既不是全额也不是挂零
                    if (flastNum == null) {
                        fnewNum = 0L;
                    } else {
                        fnewNum = flastNum;
                    }
                    if (zlastNum == null) {
                        znewNum = 0L;
                    } else {
                        znewNum = zlastNum;
                    }
                }
            }
            //各分区总人力
            if (tlastNum != null) {
                tnewNum = tlastNum + 1L;
            } else {
                tnewNum = 1L;
            }
            //全额获得率
            fmrm = (double) fnewNum / tnewNum;
            //挂零率
            zmrm = (double) znewNum / tnewNum;
            //保留两位小数

            double fmrmResultRate = new BigDecimal(fmrm).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            double zmrmResultRate = new BigDecimal(zmrm).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            //key_id和day_id
            long time = System.currentTimeMillis();
            String formatTime = formatter.format(time);
            String dayId = formatTime.substring(0, 10);
            String keyId = dayId + "#" + newFactwagebase.getProvincecom_code();
            //封装数据
            AssessmentResult assessmentResult = new AssessmentResult();
            assessmentResult.setKey_id(keyId);
            assessmentResult.setDay_id(dayId);
            assessmentResult.setManage_code(newFactwagebase.getProvincecom_code());
            assessmentResult.setManage_name(newFactwagebase.getProvincecom_name());
            assessmentResult.setAgent_grade(newFactwagebase.getAgent_grade());
            assessmentResult.setManpower_assessment_month(tnewNum.toString());
            assessmentResult.setFull_manpower_month(fnewNum.toString());
            assessmentResult.setF_manpower_rate_month(String.valueOf(fmrmResultRate));
            assessmentResult.setZero_manpower_month(znewNum.toString());
            assessmentResult.setZ_manpower_rate_month(String.valueOf(zmrmResultRate));
            assessmentResult.setLoad_date(formatTime);

            //更新状态
            khxzFState.put(currentTime.substring(8, 10) + "fullSalaryNumber", Long.parseLong(assessmentResult.getFull_manpower_month()));
            khxzZState.put(currentTime.substring(8, 10) + "zeroSalaryNumber", Long.parseLong(assessmentResult.getZero_manpower_month()));
            khxzTState.put(currentTime.substring(8, 10) + "totalNumber", Long.parseLong(assessmentResult.getManpower_assessment_month()));
            agentCodeSList.add(newFactwagebase.getAgent_id());
            return assessmentResult;
        }
        return null;
    }
}
