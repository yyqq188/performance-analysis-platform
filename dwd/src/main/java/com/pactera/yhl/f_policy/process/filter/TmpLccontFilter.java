package com.pactera.yhl.f_policy.process.filter;

import com.pactera.yhl.entity.Lccont;
import org.apache.flink.api.common.functions.FilterFunction;

public class TmpLccontFilter implements FilterFunction<Lccont> {
    @Override
    public boolean filter(Lccont lccont) throws Exception {
        return "13".equals(lccont.getSalechnl()) || "04".equals(lccont.getSalechnl());
    }
}
