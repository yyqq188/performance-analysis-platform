package com.pactera.yhl.insurance_detail.process.filter;

import com.pactera.yhl.entity.Lbcont;
import org.apache.flink.api.common.functions.FilterFunction;

public class TmpLbcontFilter implements FilterFunction<Lbcont> {
    @Override
    public boolean filter(Lbcont lbcont) throws Exception {
        return "13".equals(lbcont.getSalechnl()) || "04".equals(lbcont.getSalechnl());
    }
}
