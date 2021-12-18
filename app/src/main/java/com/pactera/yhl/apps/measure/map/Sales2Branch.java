package com.pactera.yhl.apps.measure.map;

import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.measure.entity.Measure1;
import org.apache.flink.api.common.functions.RichMapFunction;

public class Sales2Branch extends RichMapFunction<String, Measure1> {
    @Override
    public Measure1 map(String value) throws Exception {
        return JSONObject.parseObject(value,Measure1.class);
    }
}
