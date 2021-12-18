package com.pactera.yhl.apps.measure.map;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.entity.source.T02salesinfok;
import org.apache.flink.api.common.functions.RichMapFunction;

public class SalesMapFunc extends RichMapFunction<String, T02salesinfok> {
    @Override
    public T02salesinfok map(String value) throws Exception {
        return JSON.parseObject(value,T02salesinfok.class);
    }
}
