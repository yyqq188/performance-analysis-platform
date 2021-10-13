package com.pactera.yhl.insurance_detail.process.filter;

import com.pactera.yhl.entity.Lccont;
import org.apache.flink.api.common.functions.FilterFunction;

public class TmpLccontFilter implements FilterFunction<Lccont> {
    @Override
    public boolean filter(Lccont lccont) throws Exception {
        return lccont.getSalechnl().equals("13") || lccont.getSalechnl().equals("04");
    }
}
