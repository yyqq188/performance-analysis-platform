package map;

import config.TableName;
import entity.KLEntity;
import entity.Anychatcont;

public class EntityStrategy {
    public static KLEntity getEntity(String tableName){
        if(tableName == null || tableName.isEmpty()){
            throw new IllegalArgumentException("tablename should not empty");
        }
        if(tableName.equals(TableName.AnyChatCont)){

            return new Anychatcont();
        }
        return null;
    }

}
