package com.pactera.yhl.insurance_detail.process.map;

import com.pactera.yhl.entity.Lcpol;
import com.pactera.yhl.entity.Lcpol_sourceId;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;

public class TmpLcpolMap implements MapFunction<Lcpol, Lcpol_sourceId> {
    @Override
    public Lcpol_sourceId map(Lcpol lcpol) throws Exception {
        Lcpol_sourceId lcpol_sourceId = new Lcpol_sourceId();
        BeanUtils.copyProperties(lcpol_sourceId,lcpol);
        lcpol_sourceId.setSourceId("0");

        switch (lcpol.getStateflag()){
            case "0": lcpol_sourceId.setStateName("投保");
            case "1": lcpol_sourceId.setStateName("承保有效");
            case "2": lcpol_sourceId.setStateName("失效中止");
            case "3": lcpol_sourceId.setStateName("终止");
            case "4": lcpol_sourceId.setStateName("理赔终止");
        }
        return lcpol_sourceId;
    }
}
