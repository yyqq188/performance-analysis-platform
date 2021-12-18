package com.pactera.yhl.apps.measure.map.lcpol;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.entity.source.Lcpol;
import org.apache.flink.api.common.functions.RichMapFunction;

public class LcpMapFunc extends RichMapFunction<String, Lcpol> {
    public static Lcpol lcpol = null;
    @Override
    public Lcpol map(String value) throws Exception {
        lcpol = JSON.parseObject(value,Lcpol.class);
        return lcpol;
    }
}
