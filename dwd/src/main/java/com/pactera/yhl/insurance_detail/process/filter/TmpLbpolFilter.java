package com.pactera.yhl.insurance_detail.process.filter;

import com.pactera.yhl.entity.Lbpol;
import org.apache.flink.api.common.functions.FilterFunction;

public class TmpLbpolFilter implements FilterFunction<Lbpol> {
    @Override
    public boolean filter(Lbpol lbpol) throws Exception {
        return lbpol.getSalechnl().equals("13") || lbpol.getSalechnl().equals("04");
    }
}
