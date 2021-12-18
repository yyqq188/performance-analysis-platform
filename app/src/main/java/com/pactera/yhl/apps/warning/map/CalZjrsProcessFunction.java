package com.pactera.yhl.apps.warning.map;

import com.pactera.yhl.apps.warning.entity.DirectorResult;
import com.pactera.yhl.apps.warning.entity.FactWageBase;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author SUN KI
 * @time 2021/11/16 14:48
 * @Desc 总监级别分布
 */
public class CalZjrsProcessFunction extends RichMapFunction<FactWageBase, DirectorResult> {
    //1.准备一个MapState用来存放上一个窗口的Fact_Prem数据
    //MapState<"totalNum",上一个窗口的总人数>
    private MapState<String, Long> zjrsState = null;
    private static List<String> agentCodeSList = new ArrayList<>();//全局变量存agentcode
    //2.初始化State
    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, Long> stateDescriptor = new MapStateDescriptor<>("zjrsState", String.class, Long.class);
        zjrsState = getRuntimeContext().getMapState(stateDescriptor);
    }

    @Override
    public DirectorResult map(FactWageBase newFactwagebase) throws Exception {
        long l = System.currentTimeMillis();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentTime = formatter.format(l);
        String khxzFstateDate;
        String key = null;
        for (Map.Entry<String, Long> entry : zjrsState.entries()) {
            if (entry.getKey() != null) {
                key = entry.getKey();
            }
        }
        if (key != null) {
            khxzFstateDate = key.substring(0, 2);
        } else {
            khxzFstateDate = "32";
        }
        //第二天清理状态
        if (!khxzFstateDate.equals(currentTime.substring(8, 10))) {
            if (zjrsState != null){
                zjrsState.clear();
            }
            if (agentCodeSList != null){
                agentCodeSList.clear();
            }
        }
        //判断之前有没有这个代理人进来, 如果没进来过, 进行下面计算
        if (agentCodeSList == null || !agentCodeSList.contains(newFactwagebase.getAgent_id())) {
            //获取状态中上一窗口的pojo类
            Long lastNum = zjrsState.get(currentTime.substring(8, 10) + newFactwagebase.getProvincecom_code() + "#" + newFactwagebase.getAgent_grade());
            Long newNum;
            if (lastNum != null) {
                newNum = lastNum + 1L;
            } else {
                newNum = 1L;
            }

            //key_id和day_id
            long time = System.currentTimeMillis();
            String formatTime = formatter.format(time);
            String dayId = formatTime.substring(0, 10);
            String keyId = dayId + "#" + newFactwagebase.getProvincecom_code();
            //封装数据
            DirectorResult directorResult = new DirectorResult();
            directorResult.setKey_id(keyId);
            directorResult.setDay_id(dayId);
            directorResult.setManage_code(newFactwagebase.getProvincecom_code());
            directorResult.setManage_name(newFactwagebase.getProvincecom_name());
            directorResult.setDirector_num("-");
            if ("A19".equals(newFactwagebase.getAgent_grade())) {
                directorResult.setStar_director_num(newNum.toString());
            } else {
                if (zjrsState.get(newFactwagebase.getProvincecom_code() + "#A19") != null) {
                    directorResult.setStar_director_num(zjrsState.get(newFactwagebase.getProvincecom_code() + "#A19").toString());
                } else {
                    directorResult.setStar_director_num(String.valueOf(0L));
                }
            }
            if ("A18".equals(newFactwagebase.getAgent_grade())) {
                directorResult.setExperience_director_num(newNum.toString());
            } else {
                if (zjrsState.get(newFactwagebase.getProvincecom_code() + "#A18") != null) {
                    directorResult.setExperience_director_num(zjrsState.get(newFactwagebase.getProvincecom_code() + "#A18").toString());
                } else {
                    directorResult.setExperience_director_num(String.valueOf(0L));
                }

            }
            if ("A17".equals(newFactwagebase.getAgent_grade())) {
                directorResult.setSenior_director_num(newNum.toString());
            } else {
                if (zjrsState.get(newFactwagebase.getProvincecom_code() + "#A17") != null) {
                    directorResult.setSenior_director_num(zjrsState.get(newFactwagebase.getProvincecom_code() + "#A17").toString());
                } else {
                    directorResult.setSenior_director_num(String.valueOf(0L));
                }
            }
            if ("A16".equals(newFactwagebase.getAgent_grade())) {
                directorResult.setMiddle_director_num(newNum.toString());
            } else {
                if (zjrsState.get(newFactwagebase.getProvincecom_code() + "#A16") != null) {
                    directorResult.setMiddle_director_num(zjrsState.get(newFactwagebase.getProvincecom_code() + "#A16").toString());
                } else {
                    directorResult.setMiddle_director_num(String.valueOf(0L));
                }
            }
            if ("A15".equals(newFactwagebase.getAgent_grade())) {
                directorResult.setPrimary_director_num(newNum.toString());
            } else {
                if (zjrsState.get(newFactwagebase.getProvincecom_code() + "#A15") != null) {
                    directorResult.setPrimary_director_num(zjrsState.get(newFactwagebase.getProvincecom_code() + "#A15").toString());
                } else {
                    directorResult.setPrimary_director_num(String.valueOf(0L));
                }
            }
            directorResult.setLoad_date(formatTime);


            //更新状态
            zjrsState.put(currentTime.substring(8, 10) + directorResult.getManage_code() + "#" + newFactwagebase.getAgent_grade(), newNum);
            agentCodeSList.add(newFactwagebase.getAgent_id());
            return directorResult;
        }
        return null;
    }
}
