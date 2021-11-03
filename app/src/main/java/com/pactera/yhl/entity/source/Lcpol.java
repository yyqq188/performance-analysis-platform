package com.pactera.yhl.entity.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author SUN KI
 * @time 2021/9/27 14:37
 * @Desc
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Lcpol implements KLEntity{
    public String grpcontno;
    public String grppolno;
    public String contno;
    public String polno;
    public String proposalno;
    public String prtno;
    public String conttype;
    public String poltypeflag;
    public String mainpolno;
    public String masterpolno;
    public String kindcode;
    public String riskcode;
    public String riskversion;
    public String managecom;
    public String agentcom;
    public String agenttype;
    public String agentcode;
    public String agentgroup;
    public String agentcode1;
    public String salechnl;
    public String handler;
    public String insuredno;
    public String insuredname;
    public String insuredsex;
    public String insuredbirthday;
    public String insuredappage;
    public String insuredpeoples;
    public String occupationtype;
    public String appntno;
    public String appntname;
    public String cvalidate;
    public String signcom;
    public String signdate;
    public String signtime;
    public String firstpaydate;
    public String payenddate;
    public String paytodate;
    public String getstartdate;
    public String enddate;
    public String accienddate;
    public String getyearflag;
    public String getyear;
    public String payendyearflag;
    public String payendyear;
    public String insuyearflag;
    public String insuyear;
    public String acciyearflag;
    public String acciyear;
    public String getstarttype;
    public String specifyvalidate;
    public String paymode;
    public String paylocation;
    public String payintv;
    public String payyears;
    public String years;
    public String managefeerate;
    public String floatrate;
    public String premtoamnt;
    public String mult;
    public String standprem;
    public String prem;
    public String sumprem;
    public String amnt;
    public String riskamnt;
    public String leavingmoney;
    public String endorsetimes;
    public String claimtimes;
    public String livetimes;
    public String renewcount;
    public String lastgetdate;
    public String lastloandate;
    public String lastregetdate;
    public String lastedordate;
    public String lastrevdate;
    public String rnewflag;
    public String stopflag;
    public String expiryflag;
    public String autopayflag;
    public String interestdifflag;
    public String subflag;
    public String bnfflag;
    public String healthcheckflag;
    public String impartflag;
    public String reinsureflag;
    public String agentpayflag;
    public String agentgetflag;
    public String livegetmode;
    public String deadgetmode;
    public String bonusgetmode;
    public String bonusman;
    public String deadflag;
    public String smokeflag;
    public String remark;
    public String approveflag;
    public String approvecode;
    public String approvedate;
    public String approvetime;
    public String uwflag;
    public String uwcode;
    public String uwdate;
    public String uwtime;
    public String polapplydate;
    public String appflag;
    public String polstate;
    public String standbyflag1;
    public String standbyflag2;
    public String standbyflag3;
    public String operator;
    public String makedate;
    public String maketime;
    public String modifydate;
    public String modifytime;
    public String waitperiod;
    public String payrulecode;
    public String ascriptionrulecode;
    public String salechnldetail;
    public String riskseqno;
    public String copys;
    public String comfeerate;
    public String branchfeerate;
    public String proposalcontno;
    public String contplancode;
    public String cessamnt;
    public String stateflag;
    public String supplementaryprem;
    public String acctype;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;

    @Override
    public String toString() {
        return "{\"Lcpol\":{"
                + "\"grpcontno\":\""
                + grpcontno + '\"'
                + ",\"grppolno\":\""
                + grppolno + '\"'
                + ",\"contno\":\""
                + contno + '\"'
                + ",\"polno\":\""
                + polno + '\"'
                + ",\"proposalno\":\""
                + proposalno + '\"'
                + ",\"prtno\":\""
                + prtno + '\"'
                + ",\"conttype\":\""
                + conttype + '\"'
                + ",\"poltypeflag\":\""
                + poltypeflag + '\"'
                + ",\"mainpolno\":\""
                + mainpolno + '\"'
                + ",\"masterpolno\":\""
                + masterpolno + '\"'
                + ",\"kindcode\":\""
                + kindcode + '\"'
                + ",\"riskcode\":\""
                + riskcode + '\"'
                + ",\"riskversion\":\""
                + riskversion + '\"'
                + ",\"managecom\":\""
                + managecom + '\"'
                + ",\"agentcom\":\""
                + agentcom + '\"'
                + ",\"agenttype\":\""
                + agenttype + '\"'
                + ",\"agentcode\":\""
                + agentcode + '\"'
                + ",\"agentgroup\":\""
                + agentgroup + '\"'
                + ",\"agentcode1\":\""
                + agentcode1 + '\"'
                + ",\"salechnl\":\""
                + salechnl + '\"'
                + ",\"handler\":\""
                + handler + '\"'
                + ",\"insuredno\":\""
                + insuredno + '\"'
                + ",\"insuredname\":\""
                + insuredname + '\"'
                + ",\"insuredsex\":\""
                + insuredsex + '\"'
                + ",\"insuredbirthday\":\""
                + insuredbirthday + '\"'
                + ",\"insuredappage\":\""
                + insuredappage + '\"'
                + ",\"insuredpeoples\":\""
                + insuredpeoples + '\"'
                + ",\"occupationtype\":\""
                + occupationtype + '\"'
                + ",\"appntno\":\""
                + appntno + '\"'
                + ",\"appntname\":\""
                + appntname + '\"'
                + ",\"cvalidate\":\""
                + cvalidate + '\"'
                + ",\"signcom\":\""
                + signcom + '\"'
                + ",\"signdate\":\""
                + signdate + '\"'
                + ",\"signtime\":\""
                + signtime + '\"'
                + ",\"firstpaydate\":\""
                + firstpaydate + '\"'
                + ",\"payenddate\":\""
                + payenddate + '\"'
                + ",\"paytodate\":\""
                + paytodate + '\"'
                + ",\"getstartdate\":\""
                + getstartdate + '\"'
                + ",\"enddate\":\""
                + enddate + '\"'
                + ",\"accienddate\":\""
                + accienddate + '\"'
                + ",\"getyearflag\":\""
                + getyearflag + '\"'
                + ",\"getyear\":\""
                + getyear + '\"'
                + ",\"payendyearflag\":\""
                + payendyearflag + '\"'
                + ",\"payendyear\":\""
                + payendyear + '\"'
                + ",\"insuyearflag\":\""
                + insuyearflag + '\"'
                + ",\"insuyear\":\""
                + insuyear + '\"'
                + ",\"acciyearflag\":\""
                + acciyearflag + '\"'
                + ",\"acciyear\":\""
                + acciyear + '\"'
                + ",\"getstarttype\":\""
                + getstarttype + '\"'
                + ",\"specifyvalidate\":\""
                + specifyvalidate + '\"'
                + ",\"paymode\":\""
                + paymode + '\"'
                + ",\"paylocation\":\""
                + paylocation + '\"'
                + ",\"payintv\":\""
                + payintv + '\"'
                + ",\"payyears\":\""
                + payyears + '\"'
                + ",\"years\":\""
                + years + '\"'
                + ",\"managefeerate\":\""
                + managefeerate + '\"'
                + ",\"floatrate\":\""
                + floatrate + '\"'
                + ",\"premtoamnt\":\""
                + premtoamnt + '\"'
                + ",\"mult\":\""
                + mult + '\"'
                + ",\"standprem\":\""
                + standprem + '\"'
                + ",\"prem\":\""
                + prem + '\"'
                + ",\"sumprem\":\""
                + sumprem + '\"'
                + ",\"amnt\":\""
                + amnt + '\"'
                + ",\"riskamnt\":\""
                + riskamnt + '\"'
                + ",\"leavingmoney\":\""
                + leavingmoney + '\"'
                + ",\"endorsetimes\":\""
                + endorsetimes + '\"'
                + ",\"claimtimes\":\""
                + claimtimes + '\"'
                + ",\"livetimes\":\""
                + livetimes + '\"'
                + ",\"renewcount\":\""
                + renewcount + '\"'
                + ",\"lastgetdate\":\""
                + lastgetdate + '\"'
                + ",\"lastloandate\":\""
                + lastloandate + '\"'
                + ",\"lastregetdate\":\""
                + lastregetdate + '\"'
                + ",\"lastedordate\":\""
                + lastedordate + '\"'
                + ",\"lastrevdate\":\""
                + lastrevdate + '\"'
                + ",\"rnewflag\":\""
                + rnewflag + '\"'
                + ",\"stopflag\":\""
                + stopflag + '\"'
                + ",\"expiryflag\":\""
                + expiryflag + '\"'
                + ",\"autopayflag\":\""
                + autopayflag + '\"'
                + ",\"interestdifflag\":\""
                + interestdifflag + '\"'
                + ",\"subflag\":\""
                + subflag + '\"'
                + ",\"bnfflag\":\""
                + bnfflag + '\"'
                + ",\"healthcheckflag\":\""
                + healthcheckflag + '\"'
                + ",\"impartflag\":\""
                + impartflag + '\"'
                + ",\"reinsureflag\":\""
                + reinsureflag + '\"'
                + ",\"agentpayflag\":\""
                + agentpayflag + '\"'
                + ",\"agentgetflag\":\""
                + agentgetflag + '\"'
                + ",\"livegetmode\":\""
                + livegetmode + '\"'
                + ",\"deadgetmode\":\""
                + deadgetmode + '\"'
                + ",\"bonusgetmode\":\""
                + bonusgetmode + '\"'
                + ",\"bonusman\":\""
                + bonusman + '\"'
                + ",\"deadflag\":\""
                + deadflag + '\"'
                + ",\"smokeflag\":\""
                + smokeflag + '\"'
                + ",\"remark\":\""
                + remark + '\"'
                + ",\"approveflag\":\""
                + approveflag + '\"'
                + ",\"approvecode\":\""
                + approvecode + '\"'
                + ",\"approvedate\":\""
                + approvedate + '\"'
                + ",\"approvetime\":\""
                + approvetime + '\"'
                + ",\"uwflag\":\""
                + uwflag + '\"'
                + ",\"uwcode\":\""
                + uwcode + '\"'
                + ",\"uwdate\":\""
                + uwdate + '\"'
                + ",\"uwtime\":\""
                + uwtime + '\"'
                + ",\"polapplydate\":\""
                + polapplydate + '\"'
                + ",\"appflag\":\""
                + appflag + '\"'
                + ",\"polstate\":\""
                + polstate + '\"'
                + ",\"standbyflag1\":\""
                + standbyflag1 + '\"'
                + ",\"standbyflag2\":\""
                + standbyflag2 + '\"'
                + ",\"standbyflag3\":\""
                + standbyflag3 + '\"'
                + ",\"operator\":\""
                + operator + '\"'
                + ",\"makedate\":\""
                + makedate + '\"'
                + ",\"maketime\":\""
                + maketime + '\"'
                + ",\"modifydate\":\""
                + modifydate + '\"'
                + ",\"modifytime\":\""
                + modifytime + '\"'
                + ",\"waitperiod\":\""
                + waitperiod + '\"'
                + ",\"payrulecode\":\""
                + payrulecode + '\"'
                + ",\"ascriptionrulecode\":\""
                + ascriptionrulecode + '\"'
                + ",\"salechnldetail\":\""
                + salechnldetail + '\"'
                + ",\"riskseqno\":\""
                + riskseqno + '\"'
                + ",\"copys\":\""
                + copys + '\"'
                + ",\"comfeerate\":\""
                + comfeerate + '\"'
                + ",\"branchfeerate\":\""
                + branchfeerate + '\"'
                + ",\"proposalcontno\":\""
                + proposalcontno + '\"'
                + ",\"contplancode\":\""
                + contplancode + '\"'
                + ",\"cessamnt\":\""
                + cessamnt + '\"'
                + ",\"stateflag\":\""
                + stateflag + '\"'
                + ",\"supplementaryprem\":\""
                + supplementaryprem + '\"'
                + ",\"acctype\":\""
                + acctype + '\"'
                + ",\"etl_dt\":\""
                + etl_dt + '\"'
                + ",\"etl_tm\":\""
                + etl_tm + '\"'
                + ",\"etl_fg\":\""
                + etl_fg + '\"'
                + ",\"op_ts\":\""
                + op_ts + '\"'
                + ",\"current_ts\":\""
                + current_ts + '\"'
                + "}}";

    }
}
