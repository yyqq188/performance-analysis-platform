package com.pactera.yhl.apps.measure.map;

import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.measure.entity.Measure2;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/11/23 16:54
 */
public class Fact2CalMapFunc extends RichMapFunction<String, Measure2> {
    public static Measure2 measure1 = null;
    @Override
    public Measure2 map(String value) throws Exception {
        measure1 = JSONObject.parseObject(value, Measure2.class);
        return measure1;
    }
}
