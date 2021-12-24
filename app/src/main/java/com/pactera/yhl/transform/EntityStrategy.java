package com.pactera.yhl.transform;


import com.pactera.yhl.constract.TableName;
import com.pactera.yhl.entity.source.*;


public class EntityStrategy {
    public static KLEntity getEntity(String tableName){
        if(tableName == null || tableName.isEmpty()){
            throw new IllegalArgumentException("tablename should not empty");
        }


//        if(tableName.equals(TableName.Lbcont)){
//            return new Lbcont();
//        }else if(tableName.equals(TableName.Lbpol)){
//            return new Lbpol();
//        }else if(tableName.equals(TableName.Lccont)){
//            return new Lccont();
//        }else if(tableName.equals(TableName.Lcpol)){
//            return new Lcpol();
//        }

//        if(tableName.equals(TableName.AnyChatCont)){
//            return new Anychatcont();
//        }else if(tableName.equals(TableName.ImCont)){
//            return new Imcont();
//        }else if(tableName.equals(TableName.LaAgent)){
//            return new Laagent();
//        }else if(tableName.equals(TableName.LaagentCertif)){
//            return new Laagentcertif();
//        }else if(tableName.equals(TableName.LabranchGroup)){
//            return new Labranchgroup();
//        }else if(tableName.equals(TableName.LaCom)){
//            return new Lacom();
//        }else if(tableName.equals(TableName.Lbappnt)){
//            return new Lbappnt();
//        }else if(tableName.equals(TableName.Lbinsured)){
//            return new Lbinsured();
//        }else if(tableName.equals(TableName.Lcaddress)){
//            return new Lcaddress();
//        }else if(tableName.equals(TableName.Lcappnt)){
//            return new Lcappnt();
//        }else if(tableName.equals(TableName.Lccontextend)){
//            return new Lccontextend();
//        }else if(tableName.equals(TableName.Lcinsured)){
//            return new Lcinsured();
//        }else if(tableName.equals(TableName.Lcphoinfonewresult)){
//            return new Lcphoinfonewresult();
//        }else if(tableName.equals(TableName.Ldcode)){
//            return new Ldcode();
//        }else if(tableName.equals(TableName.Ldcom)){
//            return new Ldcom();
//        }else if(tableName.equals(TableName.Ldplan)){
//            return new Ldplan();
//        }else if(tableName.equals(TableName.Ljagetendorse)){
//            return new Ljagetendorse();
//        }else if(tableName.equals(TableName.Ljapayperson)){
//            return new Ljapayperson();
//        }else if(tableName.equals(TableName.Ljtempfeeclass)){
//            return new Ljtempfeeclass();
//        }else if(tableName.equals(TableName.Lktransstatus)){
//            return new Lktransstatus();
//        }else if(tableName.equals(TableName.Lmedoritem)){
//            return new Lmedoritem();
//        }else if(tableName.equals(TableName.Lmriskapp)){
//            return new Lmriskapp();
//        }else if(tableName.equals(TableName.Lpedoritem)){
//            return new Lpedoritem();
//        }else if(tableName.equals(TableName.T01bankinfoyb)){
//            return new T01bankinfoyb();
//        }else if(tableName.equals(TableName.T01branchinfo)){
//            return new T01branchinfo();
//        }else if(tableName.equals(TableName.T01teaminfo)){
//            return new T01teaminfo();
//        }else if(tableName.equals(TableName.T02salesinfo)){
//            return new T02salesinfo();
//        }else if(tableName.equals(TableName.T02salesinfo_k)){
//            return new T02salesinfo_k();
//        }
        return null;
    }

}
