package com.pactera.yhl.insurance_detail.process.filter;

import com.pactera.yhl.entity.Lcpol;
import org.apache.flink.api.common.functions.FilterFunction;

public class TmpLcpolFilter implements FilterFunction<Lcpol> {
    @Override
    public boolean filter(Lcpol lcpol) throws Exception {
        return lcpol.getSalechnl().equals("13") || lcpol.getSalechnl().equals("04");
    }
}
