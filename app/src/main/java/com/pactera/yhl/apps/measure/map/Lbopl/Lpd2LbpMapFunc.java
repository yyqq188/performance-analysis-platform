package com.pactera.yhl.apps.measure.map.Lbopl;

import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.entity.source.Lbpol;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/11/23 16:06
 */
public class Lpd2LbpMapFunc extends RichMapFunction<String, Lbpol> {
    public static Lbpol lpd2lbp = null;
    @Override
    public Lbpol map(String value) throws Exception {
        lpd2lbp = JSONObject.parseObject(value, Lbpol.class);
        return lpd2lbp;
    }
}
