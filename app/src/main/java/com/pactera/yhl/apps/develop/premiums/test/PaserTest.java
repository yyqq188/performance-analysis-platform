package com.pactera.yhl.apps.develop.premiums.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.Objects;

public class PaserTest {
    public static void ParseJson(String jsonStr){
        Object rate = null;
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        Object polno = jsonObject.get("polno");
        Object payyears = jsonObject.get("payyears");
        Object branch_name = jsonObject.get("branch_name");
        Object product_payintv = jsonObject.get("product_payintv");
        Object prem = jsonObject.get("prem");
        rate = jsonObject.get("rate");
        Object product_name = jsonObject.get("product_name");

        if(rate.toString().length() == 0){
            System.out.println("===============================");
        }


        String product_code = "";
        Object contplancode = jsonObject.get("contplancode");
        Object riskcode = jsonObject.get("riskcode");
        if(Objects.isNull(contplancode)){
            product_code = riskcode.toString();
        }else{
            product_code = contplancode.toString();
        }



        System.out.println(String.format("%s,%s,%s,%s,%s,%s,%s,%s",
                branch_name,
                product_name,
                product_code,
                product_payintv,
                prem,
                rate,
                polno,
                payyears));


    }
}
