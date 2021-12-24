package com.pactera.yhl.apps.develop.premiums.job.jobCompute.flatmap;

import com.google.common.collect.Lists;
import com.pactera.yhl.apps.develop.premiums.entity.PremiumsKafkaEntity05;
import net.sf.cglib.beans.BeanCopier;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * author: yhl
 * time: 2021/12/7 上午10:34
 * company: gientech
 */
public class OrganizationLCFlatMapCountContno extends RichFlatMapFunction<PremiumsKafkaEntity05,PremiumsKafkaEntity05> {

    MapState<String,String> mapState;
    @Override
    public void open(Configuration parameters) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(2))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();
        MapStateDescriptor descriptor = new MapStateDescriptor(
                "organiztionLcReducing11",
                String.class,
                String.class);
        descriptor.enableTimeToLive(ttlConfig);
        mapState = getRuntimeContext().getMapState(descriptor);
    }
    @Override
    public void flatMap(PremiumsKafkaEntity05 premiumsKafkaEntity05, Collector<PremiumsKafkaEntity05> collector) throws Exception {
        String contno = premiumsKafkaEntity05.getContno();
        mapState.put(contno,contno);
        if (mapState.get(contno) == null) {
            mapState.put(contno, contno);
        }
        Iterable<String> values = mapState.values();
        Iterator<String> iterator = values.iterator();
        long num = 0;
        while(iterator.hasNext()){
            System.out.println(iterator.next());
            num += 1;
        }
        System.out.println("num = " + num);
        List<Iterable<String>> lists = Arrays.asList(mapState.values());
        PremiumsKafkaEntity05 newObj = new PremiumsKafkaEntity05();
        BeanCopier beanCopier = BeanCopier.create(premiumsKafkaEntity05.getClass(), newObj.getClass(), false);
        beanCopier.copy(premiumsKafkaEntity05,newObj,null);
        newObj.setContno(String.valueOf(num));
        collector.collect(newObj);
    }
}
