package com.pactera.yhl.f_policy.process.map;

import com.pactera.yhl.entity.Lbcont;
import com.pactera.yhl.entity.Lbcont_sourceId;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class TmpLbcontMap implements MapFunction<Lbcont, Lbcont_sourceId> {
    @Override
    public Lbcont_sourceId map(Lbcont lbcont) throws Exception {
        Lbcont_sourceId lbcont_sourceId = new Lbcont_sourceId();
        BeanUtils.copyProperties(lbcont_sourceId,lbcont);
        lbcont_sourceId.setSourceId("1");
        switch (lbcont.getStateflag()){
            case "0" : lbcont_sourceId.setStateName("投保");
            case "1" : lbcont_sourceId.setStateName("承保有效");
            case "2" : lbcont_sourceId.setStateName("失效中止");
            case "3" : lbcont_sourceId.setStateName("终止");
            case "4" : lbcont_sourceId.setStateName("理赔终止");
        }
        switch (lbcont.getPaymode()){
            case "0": lbcont_sourceId.setPaymodeType("中介结算");
            case "1": lbcont_sourceId.setPaymodeType("现金");
            case "11": lbcont_sourceId.setPaymodeType("银行汇款");
            case "12": lbcont_sourceId.setPaymodeType("其他银行代收代付");
            case "13": lbcont_sourceId.setPaymodeType("赠送保费");
            case "17": lbcont_sourceId.setPaymodeType("银行缴款单");
            case "18": lbcont_sourceId.setPaymodeType("网销的“第三方”收付费");
            case "19": lbcont_sourceId.setPaymodeType("网银支付");
            case "20": lbcont_sourceId.setPaymodeType("微信支付");
            case "22": lbcont_sourceId.setPaymodeType("广州银联-批量");
            case "23": lbcont_sourceId.setPaymodeType("广州银联-实时");
            case "24": lbcont_sourceId.setPaymodeType("支付宝支付");
            case "3": lbcont_sourceId.setPaymodeType("转帐支票");
            case "4": lbcont_sourceId.setPaymodeType("银行转账");
            case "5": lbcont_sourceId.setPaymodeType("内部转帐");
            case "6": lbcont_sourceId.setPaymodeType("pos机");
        }

        return lbcont_sourceId;
    }
}
