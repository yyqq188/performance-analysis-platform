package com.pactera.yhl.hbasecdc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.*;

public class MyHBaseObserver implements RegionObserver, RegionCoprocessor {

    static Connection connection = null;
    static Table table = null;

    protected static  KafkaProducer<String,String> producer;
    static{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
                "test:2181");
        try {
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("tableName"));


            //kafka
            Properties kafkaProps = new Properties();
            kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put("bootstrap.servers","10.5.2.133:6667,10.5.2.134:6667,10.5.2.144:6667,10.5.2.145:6667");

            producer=new KafkaProducer<String, String>(kafkaProps);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private RegionCoprocessorEnvironment env = null;

    private static final String FAMAILLY_NAME = "fn";
    private static final String UID = "uid";
    private static final String BIZ = "biz";


    //2.0加入该方法，否则无法生效
    @Override
    public Optional<RegionObserver> getRegionObserver() {
        // Extremely important to be sure that the coprocessor is invoked as a RegionObserver
        return Optional.of(this);
    }

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        env = (RegionCoprocessorEnvironment) e;
    }


    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        // nothing to do here
    }
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
        try {
            String rowkey = new String(put.getRow());
            Map<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
            Map<String, Object> json = new HashMap<>();
            for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
                for (Cell cell : entry.getValue()) {
                    String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    json.put(key, value);
                }
            }
            json.put("rowkey", rowkey);
//            producer.send(new ProducerRecord<>("hbase_test_observer", JSONObject.toJSONString(json)));

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


}
