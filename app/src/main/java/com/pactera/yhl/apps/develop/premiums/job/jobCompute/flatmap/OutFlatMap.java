package com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap;

import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity02;
import com.pactera.yhl.util.Util;
import net.sf.cglib.beans.BeanCopier;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.util.Collector;

import java.beans.Beans;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * author: yhl
 * time: 2021/12/7 上午10:34
 * company: gientech
 */
public class OutFlatMap<OUT> extends RichFlatMapFunction<OUT,OUT> {
    ReducingState<Long> reducingState;
    Time ttl;
    String fieldName;
    Class<?> kafkaClazz;
    public OutFlatMap(Time ttl,String fieldName,Class<?> kafkaClazz){
        this.ttl = ttl;
        this.fieldName = fieldName;
        this.kafkaClazz = kafkaClazz;
    }
    @Override
    public void open(Configuration parameters) throws Exception {

        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(ttl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();
        ReducingStateDescriptor descriptor = new ReducingStateDescriptor("organiztionLcReducing",
                new ReduceFunction<Long>() {
                    @Override
                    public Long reduce(Long v1, Long v2) throws Exception {
                        return v1+v2;
                    }
                },Long.class);
        descriptor.enableTimeToLive(ttlConfig);
        reducingState = getRuntimeContext().getReducingState(descriptor);

    }

    

    @Override
    public void flatMap(OUT out, Collector<OUT> collector) throws Exception {
        Field field = out.getClass().getField(fieldName);
        Object o = field.get(out);
        reducingState.add(Long.valueOf(o.toString()));
        BeanCopier beanCopier = BeanCopier.create(out.getClass(), kafkaClazz, false);

        Object kafkaClazzObj = kafkaClazz.newInstance();
        beanCopier.copy(out,kafkaClazzObj,null);

        String methodName = "set"+ Util.LargerFirstChar(field.getName());
        Method method = kafkaClazz.getDeclaredMethod(methodName, String.class);
        method.invoke(kafkaClazzObj,String.valueOf(reducingState.get()));
        collector.collect((OUT) kafkaClazzObj);   //变为泛型
    }
}
