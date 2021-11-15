package com.pactera.yhl.transform;


import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.constract.TestTableName;
import com.pactera.yhl.entity.source.*;
import org.apache.flink.api.common.functions.MapFunction;

public class TestMapTransformFunc implements MapFunction<String, KLEntity> {
    @Override
    public KLEntity map(String s) throws Exception {
        String replaceStr = s.substring(1, s.length() - 1).replace("\\", "");
        JSONObject jsonObject = JSONObject.parseObject(replaceStr);
        String tableName = jsonObject.getJSONObject("meta").getString("tableName");
        String data = jsonObject.getJSONObject("data").toString();
        if(tableName.equals(TestTableName.Lbpol)){
            return JSONObject.parseObject(data, Lbpol.class);
        }else if(tableName.equals(TestTableName.Lbcont)){
            return JSONObject.parseObject(data, Lbcont.class);
        }else if(tableName.equals(TestTableName.Lcpol)){
            return JSONObject.parseObject(data, Lcpol.class);
        }else if(tableName.equals(TestTableName.Lccont)){
            return JSONObject.parseObject(data, Lccont.class);
        }else if(tableName.equals(TestTableName.T02salesinfo_k)) {
            return JSONObject.parseObject(data, T02salesinfok.class);
        }else if(tableName.equals(TestTableName.T01branchinfo)){
            return JSONObject.parseObject(data, T01branchinfo.class);
        }else if(tableName.equals(TestTableName.Lpedoritem)){
            return JSONObject.parseObject(data, Lpedoritem.class);
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
