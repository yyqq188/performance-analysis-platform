package com.pactera.yhl.apps.develop.premiums;

import com.pactera.yhl.entity.source.Lbpol;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class KeySelectorPremiums implements KeySelector<Lbpol, Tuple2<String,Double>> {
    @Override
    public Tuple2<String, Double> getKey(Lbpol lbpol) throws Exception {
        return null;
    }
}
