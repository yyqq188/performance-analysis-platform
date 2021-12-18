package com.pactera.yhl.apps.construction.statejob;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.text.SimpleDateFormat;

/**
 * @author: TSY
 * @create: 2021/11/15 0015 下午 18:10
 * @description:   日入职人数
 */
public class CountStateMap extends RichMapFunction<Tuple2<String,Long>,Tuple2<String,Long>> {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    MapState<String, Long> countState;

    @Override
    public void open(Configuration parameters) throws Exception {
        countState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>(
                "countStateCSM", String.class, Long.class
        ));
    }


    @Override
    public Tuple2<String,Long> map(Tuple2<String,Long> value) throws Exception {

        String currentTime = formatter.format(System.currentTimeMillis());
        //每天零点清理状态
        if ("00:00:00".equals(currentTime.substring(12))){
            countState.clear();
        }

        String key_id = value.f0;
        Long one = value.f1;
        if(countState.contains(key_id)){
            countState.put(key_id,countState.get(key_id) + one);
        }else {
            countState.put(key_id,one);
        }
        return Tuple2.of(key_id,countState.get(key_id));
    }
}
