package com.pactera.yhl.util.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

import java.util.LinkedList;
import java.util.List;

public class CreateTable {
    public static void main(String[] args) throws KuduException {
        //lcpol.getGrppolno(), lcpol.getPolno(), lcpol.getInsuredno(), lcpol.getPrem()
        CreateTable.createTable("lcpoltest",new String[]{"grppolno","polno","insuredno"},
                new String[]{"dayprem"});
    }

    public static void createTable(String tableName,String[] keys,String[] fields) throws KuduException {
        KuduClient client = new KuduClient.KuduClientBuilder("prod-bigdata-pc9").build();
        if(client.tableExists(tableName)){
            System.out.println("table exist");
            client.deleteTable(tableName);
            System.out.println("delete table");

        }
        List<ColumnSchema> columns = new LinkedList<ColumnSchema>();
        for (int i = 0; i <keys.length; i++) {
            columns.add(newColumn(keys[i], Type.STRING, true));
        }
        for (int i = 0; i <fields.length; i++) {
            columns.add(newColumn(fields[i], Type.STRING, false));
        }
        Schema schema = new Schema(columns);
        //创建表时提供的所有选项
        CreateTableOptions options = new CreateTableOptions();
        // 设置表的replica备份和分区规则
        List<String> parcols = new LinkedList<String>();
        for (int i = 0; i < keys.length; i++) {
            parcols.add(keys[i]);
        }
        options.setNumReplicas(1);
        //设置range分区
        options.setRangePartitionColumns(parcols);
        //设置hash分区和数量
        options.addHashPartitions(parcols, 3);
        try {
            client.createTable(tableName, schema, options);
        } catch (KuduException e) {
            e.printStackTrace();
        }finally {
            if(client.tableExists(tableName)) System.out.println("success");
            client.close();
        }
    }

    private static ColumnSchema newColumn(String name, Type type, boolean iskey) {
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        column.key(iskey);
        return column.build();
    }

}
