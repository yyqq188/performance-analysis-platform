package com.pactera.yhl.hivekafka3;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) {
        String config_path = args[0];
        Properties prop = new Properties();
        InputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(config_path));
            prop.load(in);
//            Float.valueOf("");
        }catch (Exception e){
            e.printStackTrace();
        }
        String topic = prop.getProperty("topic");
        String tables = prop.getProperty("tables");
        String limitNum = prop.getProperty("limitnum");

        System.out.println(topic+","+tables+","+limitNum);

        ExecutorService executorService = Executors.newCachedThreadPool();
        for(String tableName : tables.split(",")){
            executorService.execute(new DirectHiveToKafka(topic,tableName,prop,limitNum));
        }

        executorService.shutdown();
    }


}

