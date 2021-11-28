package com.pactera.yhl.map;


import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.entity.KLEntity;
import com.pactera.yhl.entity.Ldcode;
import org.apache.flink.api.common.functions.MapFunction;

public class TestMapTransformFunc implements MapFunction<String, KLEntity> {
    @Override
    public KLEntity map(String s) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(s);
        String tableName = jsonObject.getJSONObject("meta").getString("tableName");
        String data = jsonObject.getJSONObject("data").toString();
        if(tableName.equals(TestTableName.Ldcode)){
            return JSONObject.parseObject(data, Ldcode.class);
        }
        return null;
    }

//    @Override
//    public KLEntity map(String s) throws Exception {
////        JSONObject jsonObject = JSONObject.parseObject(s);
////        System.out.println(jsonObject);
////        tableName = jsonObject.getJSONObject("meta").getString("tableName");
////        System.out.println("===="+tableName);
////        data = jsonObject.getJSONObject("data").toString();
////        System.out.println("===="+data);
////
////        if(tableName.equals(TestTableName.Lbpol)){
////            return JSON.parseObject(data, Lbpol.class);
////        }else if(tableName.equals(TestTableName.Lbcont)){
////            return JSON.parseObject(data, Lbcont.class);
////        }else if(tableName.equals(TestTableName.Lcpol)){
////            return JSON.parseObject(data,Lbcont.class);
////        }else if(tableName.equals(TestTableName.Lccont)){
////            return JSON.parseObject(data, Lccont.class);
////        }
//        return null;
//    }
}
