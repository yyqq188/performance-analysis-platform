package com.pactera.yhl.flinkck.map;

import com.pactera.yhl.flinkck.entity.FPolicy;
import org.apache.flink.api.common.functions.MapFunction;

public class FPolicyMap implements MapFunction<String, FPolicy> {
    @Override
    public FPolicy map(String s) throws Exception {
        return null;
    }
}
