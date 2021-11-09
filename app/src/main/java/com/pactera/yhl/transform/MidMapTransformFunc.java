package com.pactera.yhl.transform;

import com.pactera.yhl.entity.source.KLEntity;
import org.apache.flink.api.common.functions.MapFunction;

public class MidMapTransformFunc implements MapFunction<String, KLEntity> {
    @Override
    public KLEntity map(String s) throws Exception {
        return null;
    }
}
