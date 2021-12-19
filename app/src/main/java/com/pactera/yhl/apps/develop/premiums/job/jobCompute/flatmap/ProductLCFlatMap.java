package com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap;

import com.pactera.yhl.apps.develop.premiums.entity.LbpolKafka06;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity04;
import net.sf.cglib.beans.BeanCopier;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * author: yhl
 * time: 2021/12/7 上午10:34
 * company: gientech
 */
public class ProductLCFlatMap extends RichFlatMapFunction<LbpolKafka06,LbpolKafka06> {
    ReducingState<Double> reducingState;

    @Override
    public void close() throws Exception {
        reducingState.clear();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(2))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();
        ReducingStateDescriptor descriptor = new ReducingStateDescriptor("organiztionLcReducing",
                new ReduceFunction<Double>() {
                    @Override
                    public Double reduce(Double v1, Double v2) throws Exception {
                        return v1+v2;
                    }
                },Double.class);
        descriptor.enableTimeToLive(ttlConfig);
        reducingState = getRuntimeContext().getReducingState(descriptor);
    }
    @Override
    public void flatMap(LbpolKafka06 lbpolKafka06, Collector<LbpolKafka06> collector) throws Exception {
        double prem = Double.valueOf(lbpolKafka06.getPrem());
        if(lbpolKafka06.getRate().equals("")
                ||lbpolKafka06.getRate().length() == 0
                || Objects.isNull(lbpolKafka06.getRate())){
            reducingState.add(prem);
        }else {
            Double rate = Double.valueOf(lbpolKafka06.getRate());
            reducingState.add(prem * rate);
        }
        LbpolKafka06 newObj = new LbpolKafka06();
        BeanCopier beanCopier = BeanCopier.create(lbpolKafka06.getClass(),
                newObj.getClass(), false);
        beanCopier.copy(lbpolKafka06,newObj,null);
        newObj.setPrem(String.valueOf(reducingState.get()));
        collector.collect(newObj);
    }
}
