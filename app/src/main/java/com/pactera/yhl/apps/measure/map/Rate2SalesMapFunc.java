package com.pactera.yhl.apps.measure.map;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.measure.entity.Rate2Lpol;
import org.apache.flink.api.common.functions.RichMapFunction;

public class Rate2SalesMapFunc extends RichMapFunction<String, Rate2Lpol> {
    public static Rate2Lpol rate2Lpol = null;
    @Override
    public Rate2Lpol map(String value) throws Exception {
        rate2Lpol = JSON.parseObject(value,Rate2Lpol.class);
        return rate2Lpol;
    }
}
