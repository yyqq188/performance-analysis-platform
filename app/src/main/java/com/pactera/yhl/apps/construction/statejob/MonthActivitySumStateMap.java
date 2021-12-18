package com.pactera.yhl.apps.construction.statejob;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.text.SimpleDateFormat;

/**
 * @author: TSY
 * @create: 2021/11/15 0015 下午 18:10
 * @description: 月期交活动人力
 */
public class MonthActivitySumStateMap extends RichMapFunction<Tuple3<String,String,String>,Tuple2<String,String>> {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    MapState<String, Double> sumState;
    MapState<String, Integer> countState;
    final static double activitySumStateMapThreshold = 10000.00;
    @Override
    public void open(Configuration parameters) throws Exception {
        sumState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>(
                "sumStateMASSM", String.class, Double.class
        ));

        countState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                "countStateMASSM", String.class, Integer.class
        ));
    }

    @Override
    public Tuple2<String, String> map(Tuple3<String, String, String> value) throws Exception {

        String currentTime = formatter.format(System.currentTimeMillis());
        //每天零点清理状态
        if ("00:00:00".equals(currentTime.substring(12))) {
            sumState.clear();
            countState.clear();
        }

        String key_id = value.f0;
        String sales_id = value.f1;
        Double CurrentPrem = Double.valueOf(value.f2);
        //Double monthSum = Double.valueOf(getHbaseResule(sales_id, "prem"));

        //存在sales_id
        if (sumState.contains(sales_id)) {
            Double historyPrem = sumState.get(sales_id);
            double sum = historyPrem + CurrentPrem;
            //大于等于1W
            if (historyPrem >= activitySumStateMapThreshold) {
                if(sum >= activitySumStateMapThreshold ){
                    sumState.put(sales_id, sum);
                }else {
                    sumState.put(sales_id, sum);
                    if(countState.get(key_id) > 0) {
                        countState.put(key_id, countState.get(key_id) - 1);
                    }else {
                        countState.put(key_id, 0);
                    }
                }
                //小于1W
            }else {
                if(sum >= activitySumStateMapThreshold ){
                    sumState.put(sales_id, sum);
                    countState.put(key_id, countState.get(key_id) + 1);
                }else {
                    sumState.put(sales_id, sum);
                }
            }
        } else {
            //不存在sales_id
            //存在日期加机构
            if (countState.contains(key_id)) {
                //sumState.put(sales_id, Double.valueOf(getHbaseResule(sales_id, "prem")) + CurrentPrem);
                //大于等于1W
                if (CurrentPrem >= activitySumStateMapThreshold) {
                    countState.put(key_id, countState.get(key_id) + 1);
                } else {
                    //小于1W
                    countState.put(key_id, countState.get(key_id) + 0);
                }
                //不存在sales_id
                //不存在日期加机构
            } else {
                //sumState.put(sales_id, monthSum + CurrentPrem);
                //大于等于1W
                if (CurrentPrem >= activitySumStateMapThreshold) {
                    countState.put(key_id, 1);
                } else {
                    //小于1W
                    countState.put(key_id, 0);
                }
            }
        }
        System.out.println("sum值： "+sumState.get(sales_id));
            return Tuple2.of(key_id, countState.get(key_id).toString());
    }
}
