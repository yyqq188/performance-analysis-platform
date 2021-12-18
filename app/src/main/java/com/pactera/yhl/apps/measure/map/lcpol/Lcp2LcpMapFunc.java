package com.pactera.yhl.apps.measure.map.lcpol;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.entity.source.Lcpol;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/11/23 16:16
 */
public class Lcp2LcpMapFunc extends RichMapFunction<String, Lcpol> {
    public static Lcpol lcpol = null;
    @Override
    public Lcpol map(String value) throws Exception {
//        value = value.substring(1,value.length()-1).replaceAll("\\\\","");
//        String data = JSON.parseObject(value).getString("data");
        lcpol = JSONObject.parseObject(value,Lcpol.class);
        return lcpol;
    }
}
