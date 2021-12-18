package com.pactera.yhl.apps.measure.map.Lbopl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.entity.source.Lbpol;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/11/23 15:47
 */
public class Lbp2LpdMapFunc extends RichMapFunction<String, Lbpol> {
    @Override
    public Lbpol map(String value) throws Exception {
//        String data = JSON.parseObject(value).getString("data");
        return JSONObject.parseObject(value, Lbpol.class);
    }
}
