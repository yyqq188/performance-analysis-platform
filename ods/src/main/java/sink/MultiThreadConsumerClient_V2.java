package sink;


import entity.KLEntity;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MultiThreadConsumerClient_V2 implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(MultiThreadConsumerClient_V2.class);
    private LinkedBlockingQueue<KLEntity> linkedBlockingQueue;
    private CyclicBarrier cyclicBarrier;
    private Connection connection;


    private List<Put> lcconts = new ArrayList<>();
    private List<Put> lccontextends = new ArrayList<>();
    private List<Put> lcinsureds = new ArrayList<>();
    private List<Put> lcphoinfonewresults = new ArrayList<>();
    private List<Put> lcpols = new ArrayList<>();



    private int lccontNum;
    private int lccontextendNum;
    private int lcinsuredNum;
    private int lcphoinfonewresultNum;
    private int lcpolNum;

    private String namespace = "kunlun:";

    private HTable lccontHTable;
    private HTable lccontextendHTable;
    private HTable lcinsuredHTable;
    private HTable lcphoinfonewresultHTable;
    private HTable lcpolHTable;

    private int barrierNum = 100;
    //列族
    private static final byte[] cf = Bytes.toBytes("f");

    public MultiThreadConsumerClient_V2(LinkedBlockingQueue<KLEntity> linkedBlockingQueue,
                                        CyclicBarrier cyclicBarrier,
                                        Map<String,String> params,
                                        Connection connection) throws IOException {
        this.linkedBlockingQueue = linkedBlockingQueue;
        this.cyclicBarrier = cyclicBarrier;
        this.connection = connection;
        this.lccontHTable = (HTable) connection.getTable(TableName.valueOf(namespace + "lccont"));
        this.lccontextendHTable = (HTable) connection.getTable(TableName.valueOf(namespace + "lccontextend"));
        this.lcinsuredHTable = (HTable) connection.getTable(TableName.valueOf(namespace + "lcinsured"));
        this.lcphoinfonewresultHTable = (HTable) connection.getTable(TableName.valueOf(namespace + "lcphoinfonewresult"));
        this.lcpolHTable = (HTable) connection.getTable(TableName.valueOf(namespace + "lcpol"));
    }

    @SneakyThrows
    @Override
    public void run() {
        while(true){
                KLEntity e = linkedBlockingQueue.poll(1,TimeUnit.SECONDS);
                if(e != null){
                    System.out.println(Thread.currentThread().getName() + e);
                    String className = e.getClass().getSimpleName();
                    switch (className) {
                        case "Lccont":
                            Put put_lccont = declareField(e, "contno");
                            if(put_lccont != null){
                                lccontHTable.put(put_lccont);
                            }
//                            lcconts.add(put_lccont);
//                            lccontNum++;
//                            if(lccontNum > barrierNum) {
//                                putDataHbase(lcconts,lccontHTable);
//                                lccontNum = 0;
//                            }

                        case "Lcpol":
                            Put put_lcpol = declareField(e, "polno");
                            if(put_lcpol != null){
                                lcpolHTable.put(put_lcpol);
                            }

//                            lcpols.add(put_lcpol);
//                            lcpolNum++;
//                            if(lcpolNum > barrierNum) {
//                                putDataHbase(lcpols,lcpolHTable);
//                                lcpolNum = 0;
//                            }
                    }
                }
        }
    }

    private void putDataHbase(List<Put> lists,HTable hTable) throws IOException {

        hTable.put(lists);
        System.out.println("2222222222");
        lists.clear();
    }

    private Put declareField(KLEntity e,String primiryKey) {
        Field[] fields = e.getClass().getDeclaredFields();
        try{
            System.out.println(primiryKey + " " + e.getClass().getDeclaredField(primiryKey).get(e).toString());
            Put put = new Put(Bytes.toBytes(e.getClass().getDeclaredField(primiryKey).get(e).toString()));
            for(Field f:fields){
                String name = f.getName();
                try{
                    put.addColumn(cf,Bytes.toBytes(name),Bytes.toBytes(f.get(e).toString()));
                }catch (Exception exception){
                    put.addColumn(cf,Bytes.toBytes(name),Bytes.toBytes("null"));
                }
            }
            return put;
        }catch (Exception exception){
            System.out.println("没有主键列名");
        }
        return null;
    }
}
