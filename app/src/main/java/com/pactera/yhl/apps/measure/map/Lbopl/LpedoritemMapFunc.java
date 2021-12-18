package com.pactera.yhl.apps.measure.map.Lbopl;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.entity.source.Lpedoritem;
import org.apache.flink.api.common.functions.RichMapFunction;

public class LpedoritemMapFunc extends RichMapFunction<String, Lpedoritem> {
    @Override
    public Lpedoritem map(String value) throws Exception {
        return JSON.parseObject(value,Lpedoritem.class);
    }
}
