package com.pactera.yhl.apps.measure.map;

import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.measure.entity.Lpol2salesinfo;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/11/23 16:36
 */
public class Sales2BranchMapFunc extends RichMapFunction<String, Lpol2salesinfo> {
    @Override
    public Lpol2salesinfo map(String value) throws Exception {
        return JSONObject.parseObject(value, Lpol2salesinfo.class);
    }
}
