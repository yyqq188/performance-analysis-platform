package com.pactera.yhl.apps.measure.map;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.measure.entity.T01teaminfo;
import org.apache.flink.api.common.functions.RichMapFunction;

public class TeamMapFunc extends RichMapFunction<String, T01teaminfo> {
    @Override
    public T01teaminfo map(String value) throws Exception {
        return JSON.parseObject(value,T01teaminfo.class);
    }
}
