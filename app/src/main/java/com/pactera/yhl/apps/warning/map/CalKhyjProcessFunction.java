package com.pactera.yhl.apps.warning.map;

import com.pactera.yhl.apps.warning.entity.FactWageBase;
import com.pactera.yhl.apps.warning.entity.GeneralResult;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author SUN KI
 * @time 2021/11/16 14:48
 * @Desc
 */
public class CalKhyjProcessFunction extends RichMapFunction<FactWageBase, GeneralResult> {
    //1.准备一个MapState用来存放上一个窗口的Fact_Prem数据
    //MapState<"totalNum",上一个窗口的总人数>
    private MapState<String, Long> upgradeState;
    private MapState<String, Long> keepState;
    private MapState<String, Long> degradeState;
    private MapState<String, Long> totalState;
    private static List<String> agentCodeSList = new ArrayList<>();//全局变量存agentcode

    //2.初始化State
    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, Long> upgradeStateDescriptor = new MapStateDescriptor<>("upgradeState", String.class, Long.class);
        MapStateDescriptor<String, Long> keepStateDescriptor = new MapStateDescriptor<>("keepState", String.class, Long.class);
        MapStateDescriptor<String, Long> degradeStateDescriptor = new MapStateDescriptor<>("degradeState", String.class, Long.class);
        MapStateDescriptor<String, Long> totalStateDescriptor = new MapStateDescriptor<>("totalState", String.class, Long.class);

        upgradeState = getRuntimeContext().getMapState(upgradeStateDescriptor);
        keepState = getRuntimeContext().getMapState(keepStateDescriptor);
        degradeState = getRuntimeContext().getMapState(degradeStateDescriptor);
        totalState = getRuntimeContext().getMapState(totalStateDescriptor);
    }

    @Override
    public GeneralResult map(FactWageBase newFactwagebase) throws Exception {
        //每天零点清理状态
        long l = System.currentTimeMillis();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentTime = formatter.format(l);
        String khxzFstateDate;
        String key = null;
        for (Map.Entry<String, Long> entry : upgradeState.entries()) {
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
            if (upgradeState != null){
                upgradeState.clear();
            }
            if (keepState != null){
                keepState.clear();
            }
            if (degradeState != null){
                degradeState.clear();
            }
            if (totalState != null){
                totalState.clear();
            }
            if (agentCodeSList != null){
                agentCodeSList.clear();
            }
        }
        //判断之前有没有这个代理人进来, 如果没进来过, 进行下面计算
        if (agentCodeSList == null || !agentCodeSList.contains(newFactwagebase.getAgent_id())) {
            //计算各分区总人数
            Long lastTotalNum = totalState.get("totalNumber");
            Long newTotalNum;

            if (lastTotalNum != null) {
                newTotalNum = lastTotalNum + 1L;
            } else {
                newTotalNum = 1L;
            }

            Long newUpgradeNum;
            Long newKeepNum;
            Long newDegradeNum;
            double degradeRateDay;
            //根据考核结果计算各状态人数
            if ("1".equals(newFactwagebase.getAssessment_result())) {
                Long lastUpgradeNum = upgradeState.get(currentTime.substring(8, 10) + newFactwagebase.getProvincecom_code() + "#1");
                if (lastUpgradeNum != null) {
                    newUpgradeNum = lastUpgradeNum + 1L;
                } else {
                    newUpgradeNum = 1L;
                }
                newKeepNum = keepState.get(currentTime.substring(8, 10) + newFactwagebase.getProvincecom_code() + "#2");
                if (newKeepNum == null) {
                    newKeepNum = 0L;
                }
                newDegradeNum = degradeState.get(currentTime.substring(8, 10) + newFactwagebase.getProvincecom_code() + "#3");
                if (newDegradeNum == null) {
                    newDegradeNum = 0L;
                }
                degradeRateDay = newDegradeNum * 1.0 / newTotalNum;
            } else if ("2".equals(newFactwagebase.getAssessment_result())) {
                Long lastKeepNum = keepState.get(currentTime.substring(8, 10) + newFactwagebase.getProvincecom_code() + "#2");
                if (lastKeepNum != null) {
                    newKeepNum = lastKeepNum + 1L;
                } else {
                    newKeepNum = 1L;
                }
                newUpgradeNum = upgradeState.get(currentTime.substring(8, 10) + newFactwagebase.getProvincecom_code() + "#1");
                if (newUpgradeNum == null) {
                    newUpgradeNum = 0L;
                }
                newDegradeNum = degradeState.get(currentTime.substring(8, 10) + newFactwagebase.getProvincecom_code() + "#3");
                if (newDegradeNum == null) {
                    newDegradeNum = 0L;
                }
                degradeRateDay = newDegradeNum * 1.0 / newTotalNum;
            } else {
                Long lastDegradeNum = degradeState.get(currentTime.substring(8, 10) + newFactwagebase.getProvincecom_code() + "#3");
                if (lastDegradeNum != null) {
                    newDegradeNum = lastDegradeNum + 1L;
                } else {
                    newDegradeNum = 1L;
                }
                newUpgradeNum = upgradeState.get(currentTime.substring(8, 10) + newFactwagebase.getProvincecom_code() + "#1");
                if (newUpgradeNum == null) {
                    newUpgradeNum = 0L;
                }
                newKeepNum = keepState.get(currentTime.substring(8, 10) + newFactwagebase.getProvincecom_code() + "#2");
                if (newKeepNum == null) {
                    newKeepNum = 0L;
                }
                degradeRateDay = newDegradeNum * 1.0 / newTotalNum;
            }

            BigDecimal b = new BigDecimal(degradeRateDay);
            double resultRate = b.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();

            //key_id和day_id
            long time = System.currentTimeMillis();
            String formatTime = formatter.format(time);
            String dayId = formatTime.substring(0, 10);
            String keyId = dayId + "#" + newFactwagebase.getProvincecom_code() + "#" + newFactwagebase.getAgent_grade();


            //封装数据
            GeneralResult general = new GeneralResult();
            general.setKey_id(keyId);
            general.setDay_id(dayId);
            general.setManage_code(newFactwagebase.getProvincecom_code());
            general.setManage_name(newFactwagebase.getProvincecom_name());
            general.setAgent_grade(newFactwagebase.getAgent_grade());
            general.setManpower_assessment_day(String.valueOf(newTotalNum));
            general.setAdvance_day(String.valueOf(newUpgradeNum));
            general.setKeep_day(String.valueOf(newKeepNum));
            general.setDegrade_day(String.valueOf(newDegradeNum));
            general.setDegrade_rate_day(String.valueOf(resultRate));//根据分公司分组的降级率
            general.setLoad_date(formatTime);

            //更新状态
            upgradeState.put(currentTime.substring(8, 10) + general.getManage_code() + "#1", newUpgradeNum);
            keepState.put(currentTime.substring(8, 10) + general.getManage_code() + "#2", newKeepNum);
            degradeState.put(currentTime.substring(8, 10) + general.getManage_code() + "#3", newDegradeNum);
            totalState.put("totalNumber", Long.parseLong(general.getManpower_assessment_day()));
            agentCodeSList.add(newFactwagebase.getAgent_id());
            //返回结果
            return general;
        }
        return null;
    }
}


    

