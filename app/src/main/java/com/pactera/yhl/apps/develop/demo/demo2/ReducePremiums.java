package com.pactera.yhl.apps.develop.demo.demo2;

import org.apache.flink.api.common.functions.ReduceFunction;
import scala.Tuple2;

public class ReducePremiums implements ReduceFunction<Tuple2<String,Long>> {
    @Override
    public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
        return null;
    }
}
