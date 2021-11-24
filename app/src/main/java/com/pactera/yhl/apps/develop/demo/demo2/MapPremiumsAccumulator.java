package com.pactera.yhl.apps.develop.demo.demo2;

import com.pactera.yhl.entity.source.Lbpol;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class MapPremiumsAccumulator extends RichMapFunction<Lbpol, Lbpol> {
    DoubleCounter counter = new DoubleCounter();

    @Override
    public void open(Configuration parameters) throws Exception {
        getRuntimeContext().addAccumulator("premiumsAccumulator",counter);
    }

    @Override
    public Lbpol map(Lbpol lbpol) throws Exception {
//        counter.add(lbpol.getPrem());
        return null;
    }
}
