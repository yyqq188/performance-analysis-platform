package com.pactera.yhl.apps.warning.map;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.warning.entity.FactWageBase;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @author SUN KI
 * @time 2021/9/27 19:52
 * @Desc
 */
public class MapFuncTableFactWageBase extends RichMapFunction<String, FactWageBase> {

    @Override
    public FactWageBase map(String value) {
        return JSON.parseObject(value, FactWageBase.class);
    }
}
