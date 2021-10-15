package com.pactera.yhl.insurance_detail.process.filter;

import com.pactera.yhl.entity.Lbpol;
import org.apache.flink.api.common.functions.FilterFunction;

public class TmpLbpolFilter implements FilterFunction<Lbpol> {
    @Override
    public boolean filter(Lbpol lbpol) throws Exception {
        return "13".equals(lbpol.getSalechnl()) || "04".equals(lbpol.getSalechnl());
    }
}
