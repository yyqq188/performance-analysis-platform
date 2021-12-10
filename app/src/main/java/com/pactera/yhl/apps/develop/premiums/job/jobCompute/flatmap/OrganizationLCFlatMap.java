package com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap;

import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity02;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity04;
import net.sf.cglib.beans.BeanCopier;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * author: yhl
 * time: 2021/12/7 上午10:34
 * company: gientech
 */
public class OrganizationLCFlatMap extends RichFlatMapFunction<PremiumsKafkaEntity04,PremiumsKafkaEntity04> {

    ReducingState<Double> reducingState;
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
    public void flatMap(PremiumsKafkaEntity04 premiumsKafkaEntity04, Collector<PremiumsKafkaEntity04> collector) throws Exception {
        reducingState.add(Double.valueOf(premiumsKafkaEntity04.getPrem()));
        PremiumsKafkaEntity04 newObj = new PremiumsKafkaEntity04();
        BeanCopier beanCopier = BeanCopier.create(premiumsKafkaEntity04.getClass(), newObj.getClass(), false);
        beanCopier.copy(premiumsKafkaEntity04,newObj,null);
        newObj.setPrem(String.valueOf(reducingState.get()));
        collector.collect(newObj);
    }
}
