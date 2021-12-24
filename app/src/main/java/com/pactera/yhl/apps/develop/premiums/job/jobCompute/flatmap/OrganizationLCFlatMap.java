package com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap;

import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity02;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity04;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity05;
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

import java.util.Objects;

/**
 * author: yhl
 * time: 2021/12/7 上午10:34
 * company: gientech
 */
public class OrganizationLCFlatMap extends RichFlatMapFunction<PremiumsKafkaEntity05,PremiumsKafkaEntity05> {

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
    public void flatMap(PremiumsKafkaEntity05 premiumsKafkaEntity05, Collector<PremiumsKafkaEntity05> collector) throws Exception {
        System.out.println("premiumsKafkaEntity05.getPolno() = " + premiumsKafkaEntity05.getPolno());
        //每次添加的时候要乘以系数
        Double prem = Double.valueOf(premiumsKafkaEntity05.getPrem());
        if(Objects.isNull(premiumsKafkaEntity05.getRate()) ||
                premiumsKafkaEntity05.getRate().equals("")
                ||premiumsKafkaEntity05.getRate().length() == 0){
            reducingState.add(prem);
        }else {
            Double rate = Double.valueOf(premiumsKafkaEntity05.getRate());
            reducingState.add(prem * rate);
        }
        PremiumsKafkaEntity05 newObj = new PremiumsKafkaEntity05();
        BeanCopier beanCopier = BeanCopier.create(premiumsKafkaEntity05.getClass(), newObj.getClass(), false);
        beanCopier.copy(premiumsKafkaEntity05,newObj,null);
        newObj.setPrem(String.valueOf(reducingState.get()));
        collector.collect(newObj);
    }
}
