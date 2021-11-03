package com.pactera.yhl.util.kudu;

import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.List;

public class SelectKudu {
    public static void main(String[] args) {
        SelectKudu.selectKudu("lcpoltest",new String[]{"grppolno","polno","insuredno","dayprem"});
    }
    public static void selectKudu(String tableName,String[] fields){
        try {
            KuduClient client = new KuduClient.KuduClientBuilder("prod-bigdata-pc9")
                    .defaultAdminOperationTimeoutMs(60000).build();
            // 获取需要查询数据的列
            List<String> projectColumns = new ArrayList<String>();
            for (int i = 0; i < fields.length; i++) {
                projectColumns.add(fields[i].toString());
            }
            KuduTable table = client.openTable(tableName);
            // 简单的读取
            KuduScanner scanner = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns).build();
            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                // 15个tablet，每次从tablet中获取的数据的行数
                int numRows = results.getNumRows();

                while (results.hasNext()) {
                    RowResult result = results.next();
                    String[] res = new String[result.getSchema().getColumnCount()];
                    for (int i = 0; i < result.getSchema().getColumnCount(); i++) {
                        res[i] = result.getString(i);
                    }
                    System.out.println(String.join(" | ",res));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
