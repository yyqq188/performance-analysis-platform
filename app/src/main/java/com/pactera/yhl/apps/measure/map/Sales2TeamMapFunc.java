package com.pactera.yhl.apps.measure.map;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.measure.entity.Sales2Team;
import org.apache.flink.api.common.functions.RichMapFunction;

public class Sales2TeamMapFunc extends RichMapFunction<String, Sales2Team> {
    @Override
    public Sales2Team map(String value) throws Exception {
        return JSON.parseObject(value, Sales2Team.class);
    }
}
