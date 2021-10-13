package com.pactera.yhl.insurance_detail.process.filter;

import com.pactera.yhl.entity.Lbcont;
import org.apache.flink.api.common.functions.FilterFunction;

public class TmpLbcontFilter implements FilterFunction<Lbcont> {
    @Override
    public boolean filter(Lbcont lbcont) throws Exception {
        return lbcont.getSalechnl().equals("13") || lbcont.getSalechnl().equals("04");
    }
}
