package com.pactera.yhl.apps.measure.map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.apps.measure.entity.AssessmentRateConfig;
import org.apache.flink.api.common.functions.MapFunction;

public class AssRateConfigMapFunc implements MapFunction<String, AssessmentRateConfig> {
    public static AssessmentRateConfig assessment_rateConfig = null;
    @Override
    public AssessmentRateConfig map(String value) throws Exception {
        String data = JSON.parseObject(value).getString("data");
        assessment_rateConfig = JSONObject.parseObject(data, AssessmentRateConfig.class);
        return assessment_rateConfig;
    }
}
