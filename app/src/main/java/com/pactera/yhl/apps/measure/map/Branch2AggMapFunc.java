package com.pactera.yhl.apps.measure.map;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.apps.measure.entity.Measure1;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/11/23 16:49
 */
public class Branch2AggMapFunc extends RichMapFunction<String, Measure1> {
    @Override
    public Measure1 map(String value) throws Exception {
        return JSON.parseObject(value, Measure1.class);
    }
}
