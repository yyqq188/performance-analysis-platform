package com.pactera.yhl.insurance_detail.process.map;


import com.pactera.yhl.entity.Lccont;
import com.pactera.yhl.entity.Lccont_sourceId;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class TmpLccontMap implements MapFunction<Lccont, Lccont_sourceId> {
    @Override
    public Lccont_sourceId map(Lccont lccont) throws Exception {
        Lccont_sourceId lccont_sourceId = new Lccont_sourceId();
        BeanUtils.copyProperties(lccont_sourceId,lccont);
        lccont_sourceId.setSourceId("0");
        switch (lccont.getStateflag()){
            case "0" : lccont_sourceId.setStateName("投保");
            case "1" : lccont_sourceId.setStateName("承保有效");
            case "2" : lccont_sourceId.setStateName("失效中止");
            case "3" : lccont_sourceId.setStateName("终止");
            case "4" : lccont_sourceId.setStateName("理赔终止");
        }
        switch (lccont.getPaymode()){
            case "0": lccont_sourceId.setPaymodeType("中介结算");
            case "1": lccont_sourceId.setPaymodeType("现金");
            case "11": lccont_sourceId.setPaymodeType("银行汇款");
            case "12": lccont_sourceId.setPaymodeType("其他银行代收代付");
            case "13": lccont_sourceId.setPaymodeType("赠送保费");
            case "17": lccont_sourceId.setPaymodeType("银行缴款单");
            case "18": lccont_sourceId.setPaymodeType("网销的“第三方”收付费");
            case "19": lccont_sourceId.setPaymodeType("网银支付");
            case "20": lccont_sourceId.setPaymodeType("微信支付");
            case "22": lccont_sourceId.setPaymodeType("广州银联-批量");
            case "23": lccont_sourceId.setPaymodeType("广州银联-实时");
            case "24": lccont_sourceId.setPaymodeType("支付宝支付");
            case "3": lccont_sourceId.setPaymodeType("转帐支票");
            case "4": lccont_sourceId.setPaymodeType("银行转账");
            case "5": lccont_sourceId.setPaymodeType("内部转帐");
            case "6": lccont_sourceId.setPaymodeType("pos机");
        }

        return lccont_sourceId;
    }
}