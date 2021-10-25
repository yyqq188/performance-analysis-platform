package com.pactera.yhl.f_agent.process.filter;

import com.pactera.yhl.entity.Lbcont;
import com.pactera.yhl.entity.T02salesinfo_k;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @author: TSY
 * @create: 2021/10/20 0020 下午 16:44
 * @description:
 */
public class TmpOXTOFilter implements FilterFunction<T02salesinfo_k> {
    @Override
    public boolean filter(T02salesinfo_k t02salesinfo_k) throws Exception {
        return "08".equals(t02salesinfo_k.getChannel_id()) || "2021".equals(t02salesinfo_k.getVersion_id());
    }
}
