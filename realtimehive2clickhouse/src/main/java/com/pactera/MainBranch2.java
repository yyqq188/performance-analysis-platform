package com.pactera;

import com.pactera.yhl.Job;
import com.pactera.yhl.demo.ReadHive;
import com.pactera.yhl.demo.WriteClickhouse;

import java.util.concurrent.LinkedBlockingQueue;

public class MainBranch2 {

    public static void main(String[] args) throws Exception {
        LinkedBlockingQueue<Object> queueCap= new LinkedBlockingQueue<>(10000);
        String hiveTableName = "kl_core.ldcode"; //ljapayperson
        new ReadHive(hiveTableName,queueCap).run();
        WriteClickhouse stringWriteClickhouse = new WriteClickhouse();
        while(true){
            stringWriteClickhouse.write(queueCap.take().toString());
        }
    }
}
