package com.pactera.yhl.apps.warning.map;



import com.pactera.yhl.apps.warning.config.TableNameV2;
import com.pactera.yhl.apps.warning.entity.T02salesinfo_k;
import com.pactera.yhl.entity.source.*;

public class EntityStrategyV2 {
    public static KLEntity getEntity(String tableName){
        if(tableName == null || tableName.isEmpty()){
            throw new IllegalArgumentException("tablename should not empty");
        }
//        if(tableName.equals(TableNameV2.Lbcont)){
//            return new Lbcont();
//        }else

        if(tableName.equals(TableNameV2.Lbpol)){
            return new Lbpol();
        }else if(tableName.equals(TableNameV2.Lccont)){
            return new Lccont();
        }else if(tableName.equals(TableNameV2.Lcpol)){
            return new Lcpol();
        }else if(tableName.equals(TableNameV2.Lpedoritem)){
            return new Lpedoritem();
        }else if(tableName.equals(TableNameV2.T01branchinfo)){
            return new T01branchinfo();
        }else if(tableName.equals(TableNameV2.T02salesinfo_k)){
            return new T02salesinfo_k();
        }
        return null;
    }

}
