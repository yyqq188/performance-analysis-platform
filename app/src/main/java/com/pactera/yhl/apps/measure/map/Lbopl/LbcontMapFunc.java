package com.pactera.yhl.apps.measure.map.Lbopl;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.entity.source.Lbcont;
import org.apache.flink.api.common.functions.RichMapFunction;

public class LbcontMapFunc extends RichMapFunction<String, Lbcont> {
    @Override
    public Lbcont map(String value) throws Exception {
        return JSON.parseObject(value,Lbcont.class);
    }
}
