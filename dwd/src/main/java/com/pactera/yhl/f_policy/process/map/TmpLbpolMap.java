package com.pactera.yhl.f_policy.process.map;

import com.pactera.yhl.entity.Lbpol;
import com.pactera.yhl.entity.Lbpol_sourceId;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class TmpLbpolMap implements MapFunction<Lbpol, Lbpol_sourceId> {
    @Override
    public Lbpol_sourceId map(Lbpol lbpol) throws Exception {
        //对select的字段进行修改和填充
        Lbpol_sourceId lbpol_sourceId = new Lbpol_sourceId();
        BeanUtils.copyProperties(lbpol_sourceId,lbpol);
        lbpol_sourceId.setSourceId("1");
        switch (lbpol.getStateflag()){
            case "0": lbpol_sourceId.setStateName("投保");
            case "1": lbpol_sourceId.setStateName("承保有效");
            case "2": lbpol_sourceId.setStateName("失效中止");
            case "3": lbpol_sourceId.setStateName("终止");
            case "4": lbpol_sourceId.setStateName("理赔终止");
        }

        return lbpol_sourceId;
    }
}
