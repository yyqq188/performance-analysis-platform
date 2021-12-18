package com.pactera.yhl.apps.measure.map.lcpol;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.entity.source.Lccont;
import org.apache.flink.api.common.functions.RichMapFunction;

public class LccontMapFunc extends RichMapFunction<String, Lccont> {
    @Override
    public Lccont map(String value) throws Exception {
        Lccont lccont = JSON.parseObject(value, Lccont.class);
        return lccont;
    }
}
