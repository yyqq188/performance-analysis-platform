package com.pactera.yhl.map;


import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.entity.*;
import org.apache.flink.api.common.functions.MapFunction;

public class TestMapTransformFunc implements MapFunction<String, KLEntity> {
    @Override
    public KLEntity map(String s) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(s);
        String tableName = jsonObject.getJSONObject("meta").getString("tableName");
        String data = jsonObject.getJSONObject("data").toString();
        if(tableName.equals(TestTableName.application_assessment_director_result)){
            return JSONObject.parseObject(data, ApplicationAssessmentDirectorResult.class);
        }else if(tableName.equals(TestTableName.application_assessment_general_result)){
            return JSONObject.parseObject(data, ApplicationGeneralResult.class);
        }else if(tableName.equals(TestTableName.application_assessment_manager_result)){
            return JSONObject.parseObject(data, ApplicationAssessmentManagerResult.class);
        }else if(tableName.equals(TestTableName.application_assessment_member_result)){
            return JSONObject.parseObject(data, ApplicationAssessmentMemberResult.class);
        }else if(tableName.equals(TestTableName.application_assessment_persion_result)){
            return JSONObject.parseObject(data, ApplicationAssessmentPersionResult.class);
        }else if(tableName.equals(TestTableName.application_assessment_salary_result)){
            return JSONObject.parseObject(data, ApplicationAssessmentSalaryResult.class);
        }else if(tableName.equals(TestTableName.application_competition_result)){
            return JSONObject.parseObject(data, ApplicationCompetitionResult.class);
        }else if(tableName.equals(TestTableName.application_director_salary_result)){
            return JSONObject.parseObject(data, ApplicationDirectorSalaryResult.class);
        }else if(tableName.equals(TestTableName.application_distribution_director_result)){
            return JSONObject.parseObject(data, ApplicationDistributionDirectorResult.class);
        }else if(tableName.equals(TestTableName.application_general_result)){
            return JSONObject.parseObject(data, ApplicationGeneralResult.class);
        }else if(tableName.equals(TestTableName.application_manager_salary_result)){
            return JSONObject.parseObject(data, ApplicationManagerSalaryResult.class);
        }else if(tableName.equals(TestTableName.application_manpower_result)){
            return JSONObject.parseObject(data, ApplicationManpowerResult.class);
        }else if(tableName.equals(TestTableName.application_member_salary_result)){
            return JSONObject.parseObject(data, ApplicationMemberSalaryResult.class);
        }else if(tableName.equals(TestTableName.application_product_detial)){
            return JSONObject.parseObject(data, ApplicationProductDetial.class);
        }else if(tableName.equals(TestTableName.application_product_result)){
            return JSONObject.parseObject(data, ApplicationProductResult.class);
        }else if(tableName.equals(TestTableName.fact_prem)){
            return JSONObject.parseObject(data, FactPrem.class);
        }
        return null;
    }

}
