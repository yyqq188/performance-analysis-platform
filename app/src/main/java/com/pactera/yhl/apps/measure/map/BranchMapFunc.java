package com.pactera.yhl.apps.measure.map;

import com.alibaba.fastjson.JSON;
import com.pactera.yhl.entity.source.T01branchinfo;
import org.apache.flink.api.common.functions.RichMapFunction;

public class BranchMapFunc extends RichMapFunction<String, T01branchinfo> {
    @Override
    public T01branchinfo map(String value) throws Exception {
        return JSON.parseObject(value,T01branchinfo.class);
    }
}
