package com.pactera.yhl.transform.normal3;


import com.pactera.yhl.entity.source.Lbpol;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class MapFuncTableHXTest extends RichMapFunction<String, Object> {

    public static Lbpol lbpol;
    static Map<String, String> metaMap = new HashMap<>();
    static Map<String, Tuple2<String, String>> clmsMap = new HashMap<>();

    @Override
    public Object map(String json) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentTs = sdf.format(System.currentTimeMillis());


        lbpol = new Lbpol();

        metaMap.clear();
        clmsMap.clear();
        String tableName;
        String type;

        try{
            
        
            JSONObject jsonObject = JSON.parseObject(json);
            String message = jsonObject.getString("message");
            JSONArray metaArr = JSON.parseObject(message).getJSONArray("meta_data");
            JSONArray columnsArr = JSON.parseObject(message).getJSONObject("columns").getJSONArray("array");
    
            if (!metaArr.isEmpty()) {
                for (Object obj : metaArr) {//遍历metaArr中的每个值，值也是json，需解析
                    JSONObject js = JSON.parseObject(String.valueOf(obj));//得到meta中的一个值，并解析成json
    
                    JSONObject name = js.getJSONObject("name");
                    String key = name.getString("string");//得到name属性对应的值，即fieldName
                    //开始解析出字段值，需要注意字段值可能为空，需要做判断
                    JSONObject val = js.getJSONObject("value");
    
                    String value;
                    if (val != null) {
                        value = val.getString("string");
                    } else {
                        value = "";
                    }
    
                    metaMap.put(key, value);
                }
            }
    
            if (!columnsArr.isEmpty()) {
                for (Object obj : columnsArr) {
                    JSONObject js = JSON.parseObject(String.valueOf(obj));
                    String fieldName = js.getJSONObject("name").getString("string").toLowerCase();
                    JSONObject val = js.getJSONObject("value");
                    JSONObject beforeVal = js.getJSONObject("beforeImage");
    
                    String value;  //更新后的值
                    String beforeValue;//更新前的值
    
                    if (val != null) {
                        value = val.getString("string");
                    } else {
                        value = " ";
                    }
    
                    if (beforeVal != null) {
                        beforeValue = beforeVal.getString("string");
                    } else {
                        beforeValue = " ";
                    }
                    clmsMap.put(fieldName, new Tuple2<>(value, beforeValue));
                }
            }
            String table = metaMap.get("INFA_TABLE_NAME");
            tableName = table.split("_")[1].toLowerCase();
    //        System.out.println("tableName = " + tableName);
            if (tableName.contains("lb")){
                System.out.println("tableName = " + tableName);
            }
            type = metaMap.get("INFA_OP_TYPE").toLowerCase();
    
    
            if (tableName.equals("lbpol")) {
                lbpol.setEdorno(clmsMap.getOrDefault("edorno", new Tuple2<>("", "")).f0);
                lbpol.setGrpcontno(clmsMap.getOrDefault("grpcontno", new Tuple2<>("", "")).f0);
                lbpol.setGrppolno(clmsMap.getOrDefault("grppolno", new Tuple2<>("", "")).f0);
                lbpol.setContno(clmsMap.getOrDefault("contno", new Tuple2<>("", "")).f0);
                lbpol.setPolno(clmsMap.getOrDefault("polno", new Tuple2<>("", "")).f0);
                lbpol.setProposalno(clmsMap.getOrDefault("proposalno", new Tuple2<>("", "")).f0);
                lbpol.setPrtno(clmsMap.getOrDefault("prtno", new Tuple2<>("", "")).f0);
                lbpol.setConttype(clmsMap.getOrDefault("conttype", new Tuple2<>("", "")).f0);
                lbpol.setPoltypeflag(clmsMap.getOrDefault("poltypeflag", new Tuple2<>("", "")).f0);
                lbpol.setMainpolno(clmsMap.getOrDefault("mainpolno", new Tuple2<>("", "")).f0);
                lbpol.setMasterpolno(clmsMap.getOrDefault("masterpolno", new Tuple2<>("", "")).f0);
                lbpol.setKindcode(clmsMap.getOrDefault("kindcode", new Tuple2<>("", "")).f0);
                lbpol.setRiskcode(clmsMap.getOrDefault("riskcode", new Tuple2<>("", "")).f0);
                lbpol.setRiskversion(clmsMap.getOrDefault("riskversion", new Tuple2<>("", "")).f0);
                lbpol.setManagecom(clmsMap.getOrDefault("managecom", new Tuple2<>("", "")).f0);
                lbpol.setAgentcom(clmsMap.getOrDefault("agentcom", new Tuple2<>("", "")).f0);
                lbpol.setAgenttype(clmsMap.getOrDefault("agenttype", new Tuple2<>("", "")).f0);
                lbpol.setAgentcode(clmsMap.getOrDefault("agentcode", new Tuple2<>("", "")).f0);
                lbpol.setAgentgroup(clmsMap.getOrDefault("agentgroup", new Tuple2<>("", "")).f0);
                lbpol.setAgentcode1(clmsMap.getOrDefault("agentcode1", new Tuple2<>("", "")).f0);
                lbpol.setSalechnl(clmsMap.getOrDefault("salechnl", new Tuple2<>("", "")).f0);
                lbpol.setHandler(clmsMap.getOrDefault("handler", new Tuple2<>("", "")).f0);
                lbpol.setInsuredno(clmsMap.getOrDefault("insuredno", new Tuple2<>("", "")).f0);
                lbpol.setInsuredname(clmsMap.getOrDefault("insuredname", new Tuple2<>("", "")).f0);
                lbpol.setInsuredsex(clmsMap.getOrDefault("insuredsex", new Tuple2<>("", "")).f0);
                String insuredbirthday = clmsMap.getOrDefault("insuredbirthday", new Tuple2<>("", "")).f0;
                if (" ".equals(insuredbirthday) || insuredbirthday == null) {
                    lbpol.setInsuredbirthday("");
                } else {
                    lbpol.setInsuredbirthday(insuredbirthday.substring(0, 4) + "-" + insuredbirthday.substring(4, 6) + "-" + insuredbirthday.substring(6, 8));
                }
                lbpol.setInsuredappage(clmsMap.getOrDefault("insuredappage", new Tuple2<>("", "")).f0);
                lbpol.setInsuredpeoples(clmsMap.getOrDefault("insuredpeoples", new Tuple2<>("", "")).f0);
                lbpol.setOccupationtype(clmsMap.getOrDefault("occupationtype", new Tuple2<>("", "")).f0);
                lbpol.setAppntname(clmsMap.getOrDefault("appntname", new Tuple2<>("", "")).f0);
                String cvalidate = clmsMap.getOrDefault("cvalidate", new Tuple2<>("", "")).f0;
                if (" ".equals(cvalidate) || cvalidate == null) {
                    lbpol.setCvalidate("");
                } else {
                    lbpol.setCvalidate(cvalidate.substring(0, 4) + "-" + cvalidate.substring(4, 6) + "-" + cvalidate.substring(6, 8));
                }
                lbpol.setSigncom(clmsMap.getOrDefault("signcom", new Tuple2<>("", "")).f0);
                String signdate = clmsMap.getOrDefault("signdate", new Tuple2<>("", "")).f0;
                if (" ".equals(signdate) || signdate == null) {
                    lbpol.setSigndate("");
                } else {
                    lbpol.setSigndate(signdate.substring(0, 4) + "-" + signdate.substring(4, 6) + "-" + signdate.substring(6, 8));
                }
                lbpol.setSigntime(clmsMap.getOrDefault("signtime", new Tuple2<>("", "")).f0);
                String firstpaydate = clmsMap.getOrDefault("firstpaydate", new Tuple2<>("", "")).f0;
                if (" ".equals(firstpaydate) || firstpaydate == null) {
                    lbpol.setFirstpaydate("");
                } else {
                    lbpol.setFirstpaydate(firstpaydate.substring(0, 4) + "-" + firstpaydate.substring(4, 6) + "-" + firstpaydate.substring(6, 8));
                }
                String payenddate = clmsMap.getOrDefault("payenddate", new Tuple2<>("", "")).f0;
                if (" ".equals(payenddate) || payenddate == null) {
                    lbpol.setPayenddate("");
                } else {
                    lbpol.setPayenddate(payenddate.substring(0, 4) + "-" + payenddate.substring(4, 6) + "-" + payenddate.substring(6, 8));
                }
                String paytodate = clmsMap.getOrDefault("paytodate", new Tuple2<>("", "")).f0;
                if (" ".equals(paytodate) || paytodate == null) {
                    lbpol.setPaytodate("");
                } else {
                    lbpol.setPaytodate(paytodate.substring(0, 4) + "-" + paytodate.substring(4, 6) + "-" + paytodate.substring(6, 8));
                }
                String getstartdate = clmsMap.getOrDefault("getstartdate", new Tuple2<>("", "")).f0;
                if (" ".equals(getstartdate) || getstartdate == null) {
                    lbpol.setGetstartdate("");
                } else {
                    lbpol.setGetstartdate(getstartdate.substring(0, 4) + "-" + getstartdate.substring(4, 6) + "-" + getstartdate.substring(6, 8));
                }
                String enddate = clmsMap.getOrDefault("enddate", new Tuple2<>("", "")).f0;
                if (" ".equals(enddate) || enddate == null) {
                    lbpol.setEnddate("");
                } else {
                    lbpol.setEnddate(enddate.substring(0, 4) + "-" + enddate.substring(4, 6) + "-" + enddate.substring(6, 8));
                }
                String accienddate = clmsMap.getOrDefault("accienddate", new Tuple2<>("", "")).f0;
                if (" ".equals(accienddate) || accienddate == null) {
                    lbpol.setAccienddate("");
                } else {
                    lbpol.setAccienddate(accienddate.substring(0, 4) + "-" + accienddate.substring(4, 6) + "-" + accienddate.substring(6, 8));
                }
                lbpol.setGetyearflag(clmsMap.getOrDefault("getyearflag", new Tuple2<>("", "")).f0);
                lbpol.setGetyear(clmsMap.getOrDefault("getyear", new Tuple2<>("", "")).f0);
                lbpol.setPayendyearflag(clmsMap.getOrDefault("payendyearflag", new Tuple2<>("", "")).f0);
                lbpol.setPayendyear(clmsMap.getOrDefault("payendyear", new Tuple2<>("", "")).f0);
                lbpol.setInsuyearflag(clmsMap.getOrDefault("insuyearflag", new Tuple2<>("", "")).f0);
                lbpol.setInsuyear(clmsMap.getOrDefault("insuyear", new Tuple2<>("", "")).f0);
                lbpol.setAcciyearflag(clmsMap.getOrDefault("acciyearflag", new Tuple2<>("", "")).f0);
                lbpol.setAcciyear(clmsMap.getOrDefault("acciyear", new Tuple2<>("", "")).f0);
                lbpol.setGetstarttype(clmsMap.getOrDefault("getstarttype", new Tuple2<>("", "")).f0);
                lbpol.setSpecifyvalidate(clmsMap.getOrDefault("specifyvalidate", new Tuple2<>("", "")).f0);
                lbpol.setPaymode(clmsMap.getOrDefault("paymode", new Tuple2<>("", "")).f0);
                lbpol.setPaylocation(clmsMap.getOrDefault("paylocation", new Tuple2<>("", "")).f0);
                lbpol.setPayintv(clmsMap.getOrDefault("payintv", new Tuple2<>("", "")).f0);
                lbpol.setPayyears(clmsMap.getOrDefault("payyears", new Tuple2<>("", "")).f0);
                lbpol.setYears(clmsMap.getOrDefault("years", new Tuple2<>("", "")).f0);
                lbpol.setManagefeerate(clmsMap.getOrDefault("managefeerate", new Tuple2<>("", "")).f0);
                lbpol.setFloatrate(clmsMap.getOrDefault("floatrate", new Tuple2<>("", "")).f0);
                lbpol.setPremtoamnt(clmsMap.getOrDefault("premtoamnt", new Tuple2<>("", "")).f0);
                lbpol.setMult(clmsMap.getOrDefault("mult", new Tuple2<>("", "")).f0);
                lbpol.setStandprem(clmsMap.getOrDefault("standprem", new Tuple2<>("", "")).f0);
                lbpol.setPrem(clmsMap.getOrDefault("prem", new Tuple2<>("", "")).f0);
                lbpol.setSumprem(clmsMap.getOrDefault("sumprem", new Tuple2<>("", "")).f0);
                lbpol.setAmnt(clmsMap.getOrDefault("amnt", new Tuple2<>("", "")).f0);
                lbpol.setRiskamnt(clmsMap.getOrDefault("riskamnt", new Tuple2<>("", "")).f0);
                lbpol.setLeavingmoney(clmsMap.getOrDefault("leavingmoney", new Tuple2<>("", "")).f0);
                lbpol.setEndorsetimes(clmsMap.getOrDefault("endorsetimes", new Tuple2<>("", "")).f0);
                lbpol.setClaimtimes(clmsMap.getOrDefault("claimtimes", new Tuple2<>("", "")).f0);
                lbpol.setLivetimes(clmsMap.getOrDefault("livetimes", new Tuple2<>("", "")).f0);
                lbpol.setRenewcount(clmsMap.getOrDefault("renewcount", new Tuple2<>("", "")).f0);
                String lastgetdate = clmsMap.getOrDefault("lastgetdate", new Tuple2<>("", "")).f0;
                if (" ".equals(lastgetdate) || lastgetdate == null) {
                    lbpol.setLastgetdate("");
                } else {
                    lbpol.setLastgetdate(lastgetdate.substring(0, 4) + "-" + lastgetdate.substring(4, 6) + "-" + lastgetdate.substring(6, 8));
                }
                String lastloandate = clmsMap.getOrDefault("lastloandate", new Tuple2<>("", "")).f0;
                if (" ".equals(lastloandate) || lastloandate == null) {
                    lbpol.setLastloandate("");
                } else {
                    lbpol.setLastloandate(lastloandate.substring(0, 4) + "-" + lastloandate.substring(4, 6) + "-" + lastloandate.substring(6, 8));
                }
                String lastregetdate = clmsMap.getOrDefault("lastregetdate", new Tuple2<>("", "")).f0;
                if (" ".equals(lastregetdate) || lastregetdate == null) {
                    lbpol.setLastregetdate("");
                } else {
                    lbpol.setLastregetdate(lastregetdate.substring(0, 4) + "-" + lastregetdate.substring(4, 6) + "-" + lastregetdate.substring(6, 8));
                }
                String lastedordate = clmsMap.getOrDefault("lastedordate", new Tuple2<>("", "")).f0;
                if (" ".equals(lastedordate) || lastedordate == null) {
                    lbpol.setLastedordate("");
                } else {
                    lbpol.setLastedordate(lastedordate.substring(0, 4) + "-" + lastedordate.substring(4, 6) + "-" + lastedordate.substring(6, 8));
                }
                String lastrevdate = clmsMap.getOrDefault("lastrevdate", new Tuple2<>("", "")).f0;
                if (" ".equals(lastrevdate) || lastrevdate == null) {
                    lbpol.setLastrevdate("");
                } else {
                    lbpol.setLastrevdate(lastrevdate.substring(0, 4) + "-" + lastrevdate.substring(4, 6) + "-" + lastrevdate.substring(6, 8));
                }
                lbpol.setRnewflag(clmsMap.getOrDefault("rnewflag", new Tuple2<>("", "")).f0);
                lbpol.setStopflag(clmsMap.getOrDefault("stopflag", new Tuple2<>("", "")).f0);
                lbpol.setExpiryflag(clmsMap.getOrDefault("expiryflag", new Tuple2<>("", "")).f0);
                lbpol.setAutopayflag(clmsMap.getOrDefault("autopayflag", new Tuple2<>("", "")).f0);
                lbpol.setInterestdifflag(clmsMap.getOrDefault("interestdifflag", new Tuple2<>("", "")).f0);
                lbpol.setSubflag(clmsMap.getOrDefault("subflag", new Tuple2<>("", "")).f0);
                lbpol.setBnfflag(clmsMap.getOrDefault("bnfflag", new Tuple2<>("", "")).f0);
                lbpol.setHealthcheckflag(clmsMap.getOrDefault("healthcheckflag", new Tuple2<>("", "")).f0);
                lbpol.setImpartflag(clmsMap.getOrDefault("impartflag", new Tuple2<>("", "")).f0);
                lbpol.setReinsureflag(clmsMap.getOrDefault("reinsureflag", new Tuple2<>("", "")).f0);
                lbpol.setAgentpayflag(clmsMap.getOrDefault("agentpayflag", new Tuple2<>("", "")).f0);
                lbpol.setAgentgetflag(clmsMap.getOrDefault("agentgetflag", new Tuple2<>("", "")).f0);
                lbpol.setLivegetmode(clmsMap.getOrDefault("livegetmode", new Tuple2<>("", "")).f0);
                lbpol.setDeadgetmode(clmsMap.getOrDefault("deadgetmode", new Tuple2<>("", "")).f0);
                lbpol.setBonusgetmode(clmsMap.getOrDefault("bonusgetmode", new Tuple2<>("", "")).f0);
                lbpol.setBonusman(clmsMap.getOrDefault("bonusman", new Tuple2<>("", "")).f0);
                lbpol.setDeadflag(clmsMap.getOrDefault("deadflag", new Tuple2<>("", "")).f0);
                lbpol.setSmokeflag(clmsMap.getOrDefault("smokeflag", new Tuple2<>("", "")).f0);
                lbpol.setRemark(clmsMap.getOrDefault("remark", new Tuple2<>("", "")).f0);
                lbpol.setApproveflag(clmsMap.getOrDefault("approveflag", new Tuple2<>("", "")).f0);
                lbpol.setApprovecode(clmsMap.getOrDefault("approvecode", new Tuple2<>("", "")).f0);
                String approvedate = clmsMap.getOrDefault("approvedate", new Tuple2<>("", "")).f0;
                if (" ".equals(approvedate) || approvedate == null) {
                    lbpol.setApprovedate("");
                } else {
                    lbpol.setApprovedate(approvedate.substring(0, 4) + "-" + approvedate.substring(4, 6) + "-" + approvedate.substring(6, 8));
                }
                lbpol.setApprovetime(clmsMap.getOrDefault("approvetime", new Tuple2<>("", "")).f0);
                lbpol.setUwflag(clmsMap.getOrDefault("uwflag", new Tuple2<>("", "")).f0);
                lbpol.setUwcode(clmsMap.getOrDefault("uwcode", new Tuple2<>("", "")).f0);
                String uwdate = clmsMap.getOrDefault("uwdate", new Tuple2<>("", "")).f0;
                if (" ".equals(uwdate) || uwdate == null) {
                    lbpol.setUwdate("");
                } else {
                    lbpol.setUwdate(uwdate.substring(0, 4) + "-" + uwdate.substring(4, 6) + "-" + uwdate.substring(6, 8));
                }
                lbpol.setUwtime(clmsMap.getOrDefault("uwtime", new Tuple2<>("", "")).f0);
                String polapplydate = clmsMap.getOrDefault("polapplydate", new Tuple2<>("", "")).f0;
                if (" ".equals(polapplydate) || polapplydate == null) {
                    lbpol.setPolapplydate("");
                } else {
                    lbpol.setPolapplydate(polapplydate.substring(0, 4) + "-" + polapplydate.substring(4, 6) + "-" + polapplydate.substring(6, 8));
                }
                lbpol.setAppflag(clmsMap.getOrDefault("appflag", new Tuple2<>("", "")).f0);
                lbpol.setPolstate(clmsMap.getOrDefault("polstate", new Tuple2<>("", "")).f0);
                lbpol.setStandbyflag1(clmsMap.getOrDefault("standbyflag1", new Tuple2<>("", "")).f0);
                lbpol.setStandbyflag2(clmsMap.getOrDefault("standbyflag2", new Tuple2<>("", "")).f0);
                lbpol.setStandbyflag3(clmsMap.getOrDefault("standbyflag3", new Tuple2<>("", "")).f0);
                lbpol.setOperator(clmsMap.getOrDefault("operator", new Tuple2<>("", "")).f0);
                String makedate = clmsMap.getOrDefault("makedate", new Tuple2<>("", "")).f0;
                if (" ".equals(makedate) || makedate == null) {
                    lbpol.setMakedate("");
                } else {
                    lbpol.setMakedate(makedate.substring(0, 4) + "-" + makedate.substring(4, 6) + "-" + makedate.substring(6, 8));
                }
                lbpol.setMaketime(clmsMap.getOrDefault("maketime", new Tuple2<>("", "")).f0);
                String modifydate = clmsMap.getOrDefault("modifydate", new Tuple2<>("", "")).f0;
                if (" ".equals(modifydate) || modifydate == null) {
                    lbpol.setModifydate("");
                } else {
                    lbpol.setModifydate(modifydate.substring(0, 4) + "-" + modifydate.substring(4, 6) + "-" + modifydate.substring(6, 8));
                }
                lbpol.setModifytime(clmsMap.getOrDefault("modifytime", new Tuple2<>("", "")).f0);
                lbpol.setWaitperiod(clmsMap.getOrDefault("waitperiod", new Tuple2<>("", "")).f0);
                lbpol.setPayrulecode(clmsMap.getOrDefault("payrulecode", new Tuple2<>("", "")).f0);
                lbpol.setAscriptionrulecode(clmsMap.getOrDefault("ascriptionrulecode", new Tuple2<>("", "")).f0);
                lbpol.setSalechnldetail(clmsMap.getOrDefault("salechnldetail", new Tuple2<>("", "")).f0);
                lbpol.setRiskseqno(clmsMap.getOrDefault("riskseqno", new Tuple2<>("", "")).f0);
                lbpol.setCopys(clmsMap.getOrDefault("copys", new Tuple2<>("", "")).f0);
                lbpol.setComfeerate(clmsMap.getOrDefault("comfeerate", new Tuple2<>("", "")).f0);
                lbpol.setBranchfeerate(clmsMap.getOrDefault("branchfeerate", new Tuple2<>("", "")).f0);
                lbpol.setProposalcontno(clmsMap.getOrDefault("proposalcontno", new Tuple2<>("", "")).f0);
                lbpol.setContplancode(clmsMap.getOrDefault("contplancode", new Tuple2<>("", "")).f0);
                lbpol.setCessamnt(clmsMap.getOrDefault("cessamnt", new Tuple2<>("", "")).f0);
                lbpol.setStateflag(clmsMap.getOrDefault("stateflag", new Tuple2<>("", "")).f0);
                lbpol.setSupplementaryprem(clmsMap.getOrDefault("supplementaryprem", new Tuple2<>("", "")).f0);
                lbpol.setAcctype(clmsMap.getOrDefault("acctype", new Tuple2<>("", "")).f0);
                lbpol.setEtl_dt("");
                lbpol.setEtl_tm(clmsMap.getOrDefault("etl_tm", new Tuple2<>("", "")).f0);
                lbpol.setEtl_fg(clmsMap.getOrDefault("etl_fg", new Tuple2<>("", "")).f0);
                lbpol.setOp_ts("");
                lbpol.setCurrent_ts(currentTs);
                if ("delete_event".equals(type)) {
                    lbpol.setAppntno("delete");
                } else {
                    lbpol.setAppntno(clmsMap.getOrDefault("appntno", new Tuple2<>("", "")).f0);
                }
                return lbpol;
            }
        }catch (Exception e){
            System.out.println("e = " + e);
        }
        return null;
    }
}