package com.pactera.yhl.f_policy.process.filter;

import com.pactera.yhl.entity.Lcpol;
import org.apache.flink.api.common.functions.FilterFunction;

public class TmpLcpolFilter implements FilterFunction<Lcpol> {
    @Override
    public boolean filter(Lcpol lcpol) throws Exception {
        return  "13".equals(lcpol.getSalechnl()) || "04".equals(lcpol.getSalechnl());
    }
}
