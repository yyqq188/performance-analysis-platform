package com.pactera.yhl.hivekafka3;

import org.apache.kerby.config.Conf;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MainV2 {
    public static void main(String[] args) {
        String topic = args[0];
        boolean defaultTables = false;
        int num = 0;
        if(defaultTables){
            num = Config.tables_default.length;
        }else {
            num = Config.tables_flexiable.length;
        }
        ExecutorService executorService = Executors.newCachedThreadPool();
//        for (int i = 0; i < num; i++) {
//            executorService.execute(new DirectHiveToKafka(topic));
//
//
//        }
    }
}
