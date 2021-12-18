package com.pactera.yhl.apps.measure.map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.measure.entity.FactWageBase;
import org.apache.flink.api.common.functions.MapFunction;

public class FactMapFunc implements MapFunction<String, FactWageBase> {
    public static FactWageBase factWageBase = null;
    @Override
    public FactWageBase map(String value) throws Exception {
        factWageBase = JSONObject.parseObject(value, FactWageBase.class);
        return factWageBase;
    }
}
