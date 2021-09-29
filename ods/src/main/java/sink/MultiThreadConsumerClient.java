package sink;


import entity.KLEntity;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
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

public class MultiThreadConsumerClient implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(MultiThreadConsumerClient.class);
    private LinkedBlockingQueue<KLEntity> linkedBlockingQueue;
    private CyclicBarrier cyclicBarrier;
    private Connection connection;

    private List<Put> anychatconts = new ArrayList<>();
    private List<Put> imconts = new ArrayList<>();
    private List<Put> laagents = new ArrayList<>();
    private List<Put> laagentcertifs = new ArrayList<>();
    private List<Put> labranchgroups = new ArrayList<>();
    private List<Put> lacoms = new ArrayList<>();
    private List<Put> lbappnts = new ArrayList<>();
    private List<Put> lbconts = new ArrayList<>();
    private List<Put> lbinsureds = new ArrayList<>();
    private List<Put> lbpols = new ArrayList<>();
    private List<Put> lcaddresss = new ArrayList<>();
    private List<Put> lcappnts = new ArrayList<>();
    private List<Put> lcconts = new ArrayList<>();
    private List<Put> lccontextends = new ArrayList<>();
    private List<Put> lcinsureds = new ArrayList<>();
    private List<Put> lcphoinfonewresults = new ArrayList<>();
    private List<Put> lcpols = new ArrayList<>();
    private List<Put> ldcodes = new ArrayList<>();
    private List<Put> ldcoms = new ArrayList<>();
    private List<Put> ldplans = new ArrayList<>();
    private List<Put> ljagetendorses = new ArrayList<>();
    private List<Put> ljapaypersons = new ArrayList<>();
    private List<Put> ljtempfeeclasss = new ArrayList<>();
    private List<Put> lktransstatuss = new ArrayList<>();
    private List<Put> lmedoritems = new ArrayList<>();
    private List<Put> lmriskapps = new ArrayList<>();
    private List<Put> lpedoritems = new ArrayList<>();
    private List<Put> t01bankinfoybs = new ArrayList<>();
    private List<Put> t01branchinfos = new ArrayList<>();
    private List<Put> t01teaminfos = new ArrayList<>();
    private List<Put> t02salesinfos = new ArrayList<>();
    private List<Put> t02salesinfo_ks = new ArrayList<>();

    private int anychatcontNum;
    private int imcontNum;
    private int laagentNum;
    private int laagentcertifNum;
    private int labranchgroupNum;
    private int lacomNum;
    private int lbappntNum;
    private int lbcontNum;
    private int lbinsuredNum;
    private int lbpolNum;
    private int lcaddresNum;
    private int lcappntNum;
    private int lccontNum;
    private int lccontextendNum;
    private int lcinsuredNum;
    private int lcphoinfonewresultNum;
    private int lcpolNum;
    private int ldcodeNum;
    private int ldcomNum;
    private int ldplanNum;
    private int ljagetendorseNum;
    private int ljapaypersonNum;
    private int ljtempfeeclasNum;
    private int lktransstatuNum;
    private int lmedoritemNum;
    private int lmriskappNum;
    private int lpedoritemNum;
    private int t01bankinfoybNum;
    private int t01branchinfoNum;
    private int t01teaminfoNum;
    private int t02salesinfoNum;
    private int t02salesinfo_kNum;

    private int barrierNum = 100;
    //列族
    private static final byte[] cf = Bytes.toBytes("f");

    public MultiThreadConsumerClient(LinkedBlockingQueue<KLEntity> linkedBlockingQueue,
                                     CyclicBarrier cyclicBarrier,
                                     Map<String,String> params) throws IOException {
        this.linkedBlockingQueue = linkedBlockingQueue;
        this.cyclicBarrier = cyclicBarrier;
        this.connection = KLHbaseConnection.getHbaseHbaseConnection(params);
    }
    @SneakyThrows
    @Override
    public void run() {
        while(true){
            KLEntity e = linkedBlockingQueue.poll(50, TimeUnit.MILLISECONDS);
            if(e != null){
                switch (e.getClass().getName()) {
                    case "Anychatcont":
                        Put put_anychatcont = declareField(e, "businessno");
                        anychatconts.add(put_anychatcont);
                        anychatcontNum++;
                    case "Imcont":
                        Put put_imcont = declareField(e, "plcid");
                        imconts.add(put_imcont);
                        imcontNum++;
                    case "Laagent":
                        Put put_laagent = declareField(e, "agentcode");
                        laagents.add(put_laagent);
                        laagentNum++;
                    case "Laagentcertif":
                        Put put_laagentcertif = declareField(e, "agency_sales_id"+"channel_id");
                        laagentcertifs.add(put_laagentcertif);
                        laagentcertifNum++;
                    case "Labranchgroup":
                        Put put_labranchgroup = declareField(e, "agentgroup");
                        labranchgroups.add(put_labranchgroup);
                        labranchgroupNum++;
                    case "Lacom":
                        Put put_lacom = declareField(e, "agentcom");
                        lacoms.add(put_lacom);
                        lacomNum++;
                    case "Lbappnt":
                        Put put_lbappnt = declareField(e, "contno");
                        lbappnts.add(put_lbappnt);
                        lbappntNum++;
                    case "Lbcont":
                        Put put_lbcont = declareField(e, "contno");
                        lbconts.add(put_lbcont);
                        lbcontNum++;
                    case "Lbinsured":
                        Put put_lbinsured = declareField(e, "contno"+"insuredno");
                        lbinsureds.add(put_lbinsured);
                        lbinsuredNum++;
                    case "Lbpol":
                        Put put_lbpol = declareField(e, "polno");
                        lbpols.add(put_lbpol);
                        lbpolNum++;
                    case "Lcaddress":
                        Put put_lcaddress = declareField(e, "customerno"+"addressno");
                        lcaddresss.add(put_lcaddress);
                        lcaddresNum++;
                    case "Lcappnt":
                        Put put_lcappnt = declareField(e, "contno");
                        lcappnts.add(put_lcappnt);
                        lcaddresNum++;
                    case "Lccont":
                        Put put_lccont = declareField(e, "contno");
                        lcconts.add(put_lccont);
                        lccontNum++;
                    case "Lccontextend":
                        Put put_lccontextend = declareField(e, "contno");
                        lccontextends.add(put_lccontextend);
                        lccontextendNum++;
                    case "Lcinsured":
                        Put put_lcinsured = declareField(e, "contno"+"insuredno");
                        lcinsureds.add(put_lcinsured);
                        lcinsuredNum++;
                    case "Lcphoinfonewresult":
                        Put put_lcphoinfonewresult = declareField(e, "contno");
                        lcphoinfonewresults.add(put_lcphoinfonewresult);
                        lcphoinfonewresultNum++;
                    case "Lcpol":
                        Put put_lcpol = declareField(e, "polno");
                        lcpols.add(put_lcpol);
                        lcpolNum++;
                    case "Ldcode":
                        Put put_ldcode = declareField(e, "codetype"+"code");
                        ldcodes.add(put_ldcode);
                        ldcodeNum++;
                    case "Ldcom":
                        Put put_ldcom = declareField(e, "comcode");
                        ldcoms.add(put_ldcom);
                        ldcomNum++;
                    case "Ldplan":
                        Put put_ldplan = declareField(e, "contplancode"+"plantype"+"contplanname2"+"contplancode2");
                        ldplans.add(put_ldplan);
                        ldplanNum++;
                    case "Ljagetendorse":
                        Put put_ljagetendorse = declareField(e, "actugetno" + "endorsementno" + "feeoperationtype"
                                + "feefinatype" + "polno" + "otherno" + "dutycode" + "payplancode");
                        ljagetendorses.add(put_ljagetendorse);
                        ljagetendorseNum++;
                    case "Ljapayperson":
                        Put put_ljapayperson = declareField(e, "dutycode" + "payno" + "payplancode" + "paytype" + "polno");
                        ljapaypersons.add(put_ljapayperson);
                        ljapaypersonNum++;
                    case "Ljtempfeeclass":
                        Put put_ljtempfeeclass = declareField(e, "tempfeeno" + "paymode");
                        ljtempfeeclasss.add(put_ljtempfeeclass);
                        ljtempfeeclasNum++;
                    case "Lktransstatus":
                        Put put_lktransstatus = declareField(e, "bankcode" + "bankbranch" + "banknode" + "transno");
                        lktransstatuss.add(put_lktransstatus);
                        lktransstatuNum++;
                    case "Lmedoritem":
                        Put put_lmedoritem = declareField(e,  "edorcode" + "appobj");
                        lmedoritems.add(put_lmedoritem);
                        lmedoritemNum++;
                    case "Lmriskapp":
                        Put put_lmriskapp = declareField(e, "riskcode");
                        lmriskapps.add(put_lmriskapp);
                        lmriskappNum++;
                    case "Lpedoritem":
                        Put put_lpedoritem = declareField(e, "contno" + "edoracceptno" + "edorno"
                                + "edortype" + "insuredno" + "polno");
                        lpedoritems.add(put_lpedoritem);
                        lpedoritemNum++;
                    case "T01bankinfoyb":
                        Put put_t01bankinfoyb = declareField(e, "bank_id" + "channel_id");
                        t01bankinfoybs.add(put_t01bankinfoyb);
                        t01bankinfoybNum++;
                    case "T01branchinfo":
                        Put put_t01branchinfo = declareField(e, "branch_id");
                        t01branchinfos.add(put_t01branchinfo);
                        t01branchinfoNum++;
                    case "T01teaminfo":
                        Put put_t01teaminfo = declareField(e, "channel_id" + "team_id");
                        t01teaminfos.add(put_t01teaminfo);
                        t01teaminfoNum++;
                    case "T02salesinfo":
                        Put put_t02salesinfo = declareField(e, "sales_id");
                        t02salesinfos.add(put_t02salesinfo);
                        t02salesinfoNum++;
                    case "T02salesinfo_k":
                        Put put_t02salesinfo_k = declareField(e, "channel_id" + "branch_id" + "team_id" + "sales_id");
                        t02salesinfo_ks.add(put_t02salesinfo_k);
                        t02salesinfo_kNum++;

                }

                if(anychatcontNum > barrierNum)  putDataHbase(anychatconts,"anychatcont");
                if(imcontNum > barrierNum)  putDataHbase(imconts,"imcont");
                if(laagentNum > barrierNum) putDataHbase(laagentcertifs,"laagentcertif");
                if(laagentcertifNum > barrierNum)  putDataHbase(laagentcertifs,"laagentcertif");
                if(labranchgroupNum > barrierNum)  putDataHbase(labranchgroups,"labranchgroup");
                if(lacomNum > barrierNum)  putDataHbase(lacoms,"lacom");
                if(lbappntNum > barrierNum)  putDataHbase(lbappnts,"lbappnt");
                if(lbcontNum > barrierNum)  putDataHbase(lbconts,"lbcont");
                if(lbinsuredNum > barrierNum)  putDataHbase(lbinsureds,"lbinsured");
                if(lbpolNum > barrierNum)  putDataHbase(lbpols,"lbpol");
                if(lcaddresNum > barrierNum)  putDataHbase(lcaddresss,"lcaddress");
                if(lcappntNum > barrierNum)  putDataHbase(lcappnts,"lcappnt");
                if(lccontNum > barrierNum)  putDataHbase(lcconts,"lccont");
                if(lccontextendNum > barrierNum)  putDataHbase(lccontextends,"lccontextend");
                if(lcinsuredNum > barrierNum)  putDataHbase(lcinsureds,"lcinsured");
                if(lcphoinfonewresultNum > barrierNum) putDataHbase(lcphoinfonewresults,"lcphoinfonewresult");
                if(lcpolNum > barrierNum) putDataHbase(lcpols,"lcpol");
                if(ldcodeNum > barrierNum)  putDataHbase(ldcodes,"ldcode");
                if(ldcomNum > barrierNum)  putDataHbase(ldcoms,"ldcom");
                if(ldplanNum > barrierNum)  putDataHbase(ldplans,"ldplan");
                if(ljagetendorseNum > barrierNum)  putDataHbase(ljagetendorses,"ljagetendorse");
                if(ljapaypersonNum > barrierNum)  putDataHbase(ljapaypersons,"ljapayperson");
                if(ljtempfeeclasNum > barrierNum)  putDataHbase(ljtempfeeclasss,"ljtempfeeclass");
                if(lktransstatuNum > barrierNum)  putDataHbase(lktransstatuss,"lktransstatus");
                if(lmedoritemNum > barrierNum)  putDataHbase(lmedoritems,"lmedoritem");
                if(lmriskappNum > barrierNum)  putDataHbase(lmriskapps,"lmriskapp");
                if(lpedoritemNum > barrierNum)  putDataHbase(lpedoritems,"lpedoritem");
                if(t01bankinfoybNum > barrierNum)  putDataHbase(t01bankinfoybs,"t01bankinfoyb");
                if(t01branchinfoNum > barrierNum)  putDataHbase(t01branchinfos,"t01branchinfo");
                if(t01teaminfoNum > barrierNum)  putDataHbase(t01teaminfos,"t01teaminfo");
                if(t02salesinfoNum > barrierNum)  putDataHbase(t02salesinfos,"t02salesinfo");
                if(t02salesinfo_kNum > barrierNum)  putDataHbase(t02salesinfo_ks,"t02salesinfo_k");


            }else{
                putDataHbase(anychatconts,"anychatcont");
                putDataHbase(imconts,"imcont");
                putDataHbase(laagentcertifs,"laagentcertif");
                putDataHbase(laagentcertifs,"laagentcertif");
                putDataHbase(labranchgroups,"labranchgroup");
                putDataHbase(lacoms,"lacom");
                putDataHbase(lbappnts,"lbappnt");
                putDataHbase(lbconts,"lbcont");
                putDataHbase(lbinsureds,"lbinsured");
                putDataHbase(lbpols,"lbpol");
                putDataHbase(lcaddresss,"lcaddress");
                putDataHbase(lcappnts,"lcappnt");
                putDataHbase(lcconts,"lccont");
                putDataHbase(lccontextends,"lccontextend");
                putDataHbase(lcinsureds,"lcinsured");
                putDataHbase(lcphoinfonewresults,"lcphoinfonewresult");
                putDataHbase(lcpols,"lcpol");
                putDataHbase(ldcodes,"ldcode");
                putDataHbase(ldcoms,"ldcom");
                putDataHbase(ldplans,"ldplan");
                putDataHbase(ljagetendorses,"ljagetendorse");
                putDataHbase(ljapaypersons,"ljapayperson");
                putDataHbase(ljtempfeeclasss,"ljtempfeeclass");
                putDataHbase(lktransstatuss,"lktransstatus");
                putDataHbase(lmedoritems,"lmedoritem");
                putDataHbase(lmriskapps,"lmriskapp");
                putDataHbase(lpedoritems,"lpedoritem");
                putDataHbase(t01bankinfoybs,"t01bankinfoyb");
                putDataHbase(t01branchinfos,"t01branchinfo");
                putDataHbase(t01teaminfos,"t01teaminfo");
                putDataHbase(t02salesinfos,"t02salesinfo");
                putDataHbase(t02salesinfo_ks,"t02salesinfo_k");

                cyclicBarrier.await();

            }





        }

    }

    private void putDataHbase(List<Put> lists,String tableName){
        try (HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName))) {

            hTable.put(lists);


        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private Put declareField(KLEntity e,String primiryKey) throws IllegalAccessException {
        Field[] fields = e.getClass().getDeclaredFields();
        Put put = new Put(Bytes.toBytes(primiryKey));
        for(Field f:fields){
            String name = f.getName();
            put.addColumn(cf,Bytes.toBytes(name),Bytes.toBytes(f.get(name).toString()));
        }
        return put;
    }
}
