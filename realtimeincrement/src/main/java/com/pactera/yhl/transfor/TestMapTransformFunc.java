package com.pactera.yhl.transfor;


import com.alibaba.fastjson.JSONObject;
import com.pactera.yhl.entity.*;
import org.apache.flink.api.common.functions.MapFunction;

public class TestMapTransformFunc implements MapFunction<String, KLEntity> {
    @Override
    public KLEntity map(String s) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(s);
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
            return JSONObject.parseObject(data, T02salesinfo_k.class);
        }else if(tableName.equals(TestTableName.T01branchinfo)){
            return JSONObject.parseObject(data, T01branchinfo.class);
        }else if(tableName.equals(TestTableName.Lpedoritem)){
            return JSONObject.parseObject(data, Lpedoritem.class);
        }else if(tableName.equals(TestTableName.Ldcode)){
            return JSONObject.parseObject(data, Ldcode.class);
        }
        else if(tableName.equals(TestTableName.AnyChatCont)){
            return JSONObject.parseObject(data, Anychatcont.class);
        }

        else if(tableName.equals(TestTableName.ImCont)){
            return JSONObject.parseObject(data, Imcont.class);
        }

        else if(tableName.equals(TestTableName.LabranchGroup)){
            return JSONObject.parseObject(data, Labranchgroup.class);
        }
        else if(tableName.equals(TestTableName.LaCom)){
            return JSONObject.parseObject(data, Lacom.class);
        }
        else if(tableName.equals(TestTableName.Lbappnt)){
            return JSONObject.parseObject(data, Lbappnt.class);
        }
        else if(tableName.equals(TestTableName.Lbinsured)){
            return JSONObject.parseObject(data, Lbinsured.class);
        }
        else if(tableName.equals(TestTableName.Lcaddress)){
            return JSONObject.parseObject(data, Lcaddress.class);
        }
        else if(tableName.equals(TestTableName.Lcappnt)){
            return JSONObject.parseObject(data, Lcappnt.class);
        }
        else if(tableName.equals(TestTableName.Lccontextend)){
            return JSONObject.parseObject(data, Lccontextend.class);
        }

        else if(tableName.equals(TestTableName.Lcinsured)){
            return JSONObject.parseObject(data, Lcinsured.class);
        }
        else if(tableName.equals(TestTableName.Lcphoinfonewresult)){
            return JSONObject.parseObject(data, Lcphoinfonewresult.class);
        }
        else if(tableName.equals(TestTableName.Ldcom)){
            return JSONObject.parseObject(data, Ldcom.class);
        }
        else if(tableName.equals(TestTableName.Ldplan)){
            return JSONObject.parseObject(data, Ldplan.class);
        }
        else if(tableName.equals(TestTableName.Ljagetendorse)){
            return JSONObject.parseObject(data, Ljagetendorse.class);
        }
        else if(tableName.equals(TestTableName.Ljapayperson)){
            return JSONObject.parseObject(data, Ljapayperson.class);
        }
        else if(tableName.equals(TestTableName.Lktransstatus)){
            return JSONObject.parseObject(data, Lktransstatus.class);
        }
        else if(tableName.equals(TestTableName.Ljtempfeeclass)){
            return JSONObject.parseObject(data, Ljtempfeeclass.class);
        }
        else if(tableName.equals(TestTableName.Lmedoritem)){
            return JSONObject.parseObject(data, Lmedoritem.class);
        }
        else if(tableName.equals(TestTableName.Lmriskapp)){
            return JSONObject.parseObject(data, Lmriskapp.class);
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
