package com.pactera.yhl.flinkck.map;

import com.pactera.yhl.flinkck.entity.FAgent;
import org.apache.flink.api.common.functions.MapFunction;

public class FAgentMap implements MapFunction<String, FAgent> {
    @Override
    public FAgent map(String s) throws Exception {

        return new FAgent();
    }
}
