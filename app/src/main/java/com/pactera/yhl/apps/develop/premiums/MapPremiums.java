package com.pactera.yhl.apps.develop.premiums;

import com.pactera.yhl.entity.source.Lcpol;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class MapPremiums implements MapFunction<Lcpol, Tuple2<String,Long>> {
    @Override
    public Tuple2<String, Long> map(Lcpol lbpol) throws Exception {
        return Tuple2.of(lbpol.years,1L);
    }
}
