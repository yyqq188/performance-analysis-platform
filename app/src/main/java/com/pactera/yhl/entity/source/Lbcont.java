package com.pactera.yhl.entity.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author SUN KI
 * @time 2021/9/27 14:37
 * @Desc
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Lbcont{
    public String edorno;
    public String grpcontno;
    public String contno;
    public String proposalcontno;
    public String prtno;
    public String conttype;
    public String familytype;
    public String familyid;
    public String poltype;
    public String cardflag;
    public String managecom;
    public String executecom;
    public String agentcom;
    public String agentcode;
    public String agentgroup;
    public String agentcode1;
    public String agenttype;
    public String salechnl;
    public String handler;
    public String password;
    public String appntno;
    public String appntname;
    public String appntsex;
    public String appntbirthday;
    public String appntidtype;
    public String appntidno;
    public String insuredno;
    public String insuredname;
    public String insuredsex;
    public String insuredbirthday;
    public String insuredidtype;
    public String insuredidno;
    public String payintv;
    public String paymode;
    public String paylocation;
    public String disputedflag;
    public String outpayflag;
    public String getpolmode;
    public String signcom;
    public String signdate;
    public String signtime;
    public String consignno;
    public String bankcode;
    public String bankaccno;
    public String accname;
    public String printcount;
    public String losttimes;
    public String lang;
    public String currency;
    public String remark;
    public String peoples;
    public String mult;
    public String prem;
    public String amnt;
    public String sumprem;
    public String dif;
    public String paytodate;
    public String firstpaydate;
    public String cvalidate;
    public String inputoperator;
    public String inputdate;
    public String inputtime;
    public String approveflag;
    public String approvecode;
    public String approvedate;
    public String approvetime;
    public String uwflag;
    public String uwoperator;
    public String uwdate;
    public String uwtime;
    public String appflag;
    public String polapplydate;
    public String getpoldate;
    public String getpoltime;
    public String customgetpoldate;
    public String state;
    public String operator;
    public String makedate;
    public String maketime;
    public String modifydate;
    public String modifytime;
    public String firsttrialoperator;
    public String firsttrialdate;
    public String firsttrialtime;
    public String receiveoperator;
    public String receivedate;
    public String receivetime;
    public String tempfeeno;
    public String proposaltype;
    public String salechnldetail;
    public String contprintloflag;
    public String contpremfeeno;
    public String customerreceiptno;
    public String cinvalidate;
    public String copys;
    public String degreetype;
    public String handlerdate;
    public String handlerprint;
    public String stateflag;
    public String premscope;
    public String intlflag;
    public String uwconfirmno;
    public String payertype;
    public String grpagentcom;
    public String grpagentcode;
    public String grpagentname;
    public String acctype;
    public String crs_salechnl;
    public String crs_busstype;
    public String grpagentidno;
    public String duefeemsgflag;
    public String paymethod;
    public String xpaymode;
    public String xbankcode;
    public String xbankaccno;
    public String xaccname;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;

    @Override
    public String toString() {
        return "{\"Lbcont\":{"
                + "\"edorno\":\""
                + edorno + '\"'
                + ",\"grpcontno\":\""
                + grpcontno + '\"'
                + ",\"contno\":\""
                + contno + '\"'
                + ",\"proposalcontno\":\""
                + proposalcontno + '\"'
                + ",\"prtno\":\""
                + prtno + '\"'
                + ",\"conttype\":\""
                + conttype + '\"'
                + ",\"familytype\":\""
                + familytype + '\"'
                + ",\"familyid\":\""
                + familyid + '\"'
                + ",\"poltype\":\""
                + poltype + '\"'
                + ",\"cardflag\":\""
                + cardflag + '\"'
                + ",\"managecom\":\""
                + managecom + '\"'
                + ",\"executecom\":\""
                + executecom + '\"'
                + ",\"agentcom\":\""
                + agentcom + '\"'
                + ",\"agentcode\":\""
                + agentcode + '\"'
                + ",\"agentgroup\":\""
                + agentgroup + '\"'
                + ",\"agentcode1\":\""
                + agentcode1 + '\"'
                + ",\"agenttype\":\""
                + agenttype + '\"'
                + ",\"salechnl\":\""
                + salechnl + '\"'
                + ",\"handler\":\""
                + handler + '\"'
                + ",\"password\":\""
                + password + '\"'
                + ",\"appntno\":\""
                + appntno + '\"'
                + ",\"appntname\":\""
                + appntname + '\"'
                + ",\"appntsex\":\""
                + appntsex + '\"'
                + ",\"appntbirthday\":\""
                + appntbirthday + '\"'
                + ",\"appntidtype\":\""
                + appntidtype + '\"'
                + ",\"appntidno\":\""
                + appntidno + '\"'
                + ",\"insuredno\":\""
                + insuredno + '\"'
                + ",\"insuredname\":\""
                + insuredname + '\"'
                + ",\"insuredsex\":\""
                + insuredsex + '\"'
                + ",\"insuredbirthday\":\""
                + insuredbirthday + '\"'
                + ",\"insuredidtype\":\""
                + insuredidtype + '\"'
                + ",\"insuredidno\":\""
                + insuredidno + '\"'
                + ",\"payintv\":\""
                + payintv + '\"'
                + ",\"paymode\":\""
                + paymode + '\"'
                + ",\"paylocation\":\""
                + paylocation + '\"'
                + ",\"disputedflag\":\""
                + disputedflag + '\"'
                + ",\"outpayflag\":\""
                + outpayflag + '\"'
                + ",\"getpolmode\":\""
                + getpolmode + '\"'
                + ",\"signcom\":\""
                + signcom + '\"'
                + ",\"signdate\":\""
                + signdate + '\"'
                + ",\"signtime\":\""
                + signtime + '\"'
                + ",\"consignno\":\""
                + consignno + '\"'
                + ",\"bankcode\":\""
                + bankcode + '\"'
                + ",\"bankaccno\":\""
                + bankaccno + '\"'
                + ",\"accname\":\""
                + accname + '\"'
                + ",\"printcount\":\""
                + printcount + '\"'
                + ",\"losttimes\":\""
                + losttimes + '\"'
                + ",\"lang\":\""
                + lang + '\"'
                + ",\"currency\":\""
                + currency + '\"'
                + ",\"remark\":\""
                + remark + '\"'
                + ",\"peoples\":\""
                + peoples + '\"'
                + ",\"mult\":\""
                + mult + '\"'
                + ",\"prem\":\""
                + prem + '\"'
                + ",\"amnt\":\""
                + amnt + '\"'
                + ",\"sumprem\":\""
                + sumprem + '\"'
                + ",\"dif\":\""
                + dif + '\"'
                + ",\"paytodate\":\""
                + paytodate + '\"'
                + ",\"firstpaydate\":\""
                + firstpaydate + '\"'
                + ",\"cvalidate\":\""
                + cvalidate + '\"'
                + ",\"inputoperator\":\""
                + inputoperator + '\"'
                + ",\"inputdate\":\""
                + inputdate + '\"'
                + ",\"inputtime\":\""
                + inputtime + '\"'
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
                + ",\"uwoperator\":\""
                + uwoperator + '\"'
                + ",\"uwdate\":\""
                + uwdate + '\"'
                + ",\"uwtime\":\""
                + uwtime + '\"'
                + ",\"appflag\":\""
                + appflag + '\"'
                + ",\"polapplydate\":\""
                + polapplydate + '\"'
                + ",\"getpoldate\":\""
                + getpoldate + '\"'
                + ",\"getpoltime\":\""
                + getpoltime + '\"'
                + ",\"customgetpoldate\":\""
                + customgetpoldate + '\"'
                + ",\"state\":\""
                + state + '\"'
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
                + ",\"firsttrialoperator\":\""
                + firsttrialoperator + '\"'
                + ",\"firsttrialdate\":\""
                + firsttrialdate + '\"'
                + ",\"firsttrialtime\":\""
                + firsttrialtime + '\"'
                + ",\"receiveoperator\":\""
                + receiveoperator + '\"'
                + ",\"receivedate\":\""
                + receivedate + '\"'
                + ",\"receivetime\":\""
                + receivetime + '\"'
                + ",\"tempfeeno\":\""
                + tempfeeno + '\"'
                + ",\"proposaltype\":\""
                + proposaltype + '\"'
                + ",\"salechnldetail\":\""
                + salechnldetail + '\"'
                + ",\"contprintloflag\":\""
                + contprintloflag + '\"'
                + ",\"contpremfeeno\":\""
                + contpremfeeno + '\"'
                + ",\"customerreceiptno\":\""
                + customerreceiptno + '\"'
                + ",\"cinvalidate\":\""
                + cinvalidate + '\"'
                + ",\"copys\":\""
                + copys + '\"'
                + ",\"degreetype\":\""
                + degreetype + '\"'
                + ",\"handlerdate\":\""
                + handlerdate + '\"'
                + ",\"handlerprint\":\""
                + handlerprint + '\"'
                + ",\"stateflag\":\""
                + stateflag + '\"'
                + ",\"premscope\":\""
                + premscope + '\"'
                + ",\"intlflag\":\""
                + intlflag + '\"'
                + ",\"uwconfirmno\":\""
                + uwconfirmno + '\"'
                + ",\"payertype\":\""
                + payertype + '\"'
                + ",\"grpagentcom\":\""
                + grpagentcom + '\"'
                + ",\"grpagentcode\":\""
                + grpagentcode + '\"'
                + ",\"grpagentname\":\""
                + grpagentname + '\"'
                + ",\"acctype\":\""
                + acctype + '\"'
                + ",\"crs_salechnl\":\""
                + crs_salechnl + '\"'
                + ",\"crs_busstype\":\""
                + crs_busstype + '\"'
                + ",\"grpagentidno\":\""
                + grpagentidno + '\"'
                + ",\"duefeemsgflag\":\""
                + duefeemsgflag + '\"'
                + ",\"paymethod\":\""
                + paymethod + '\"'
                + ",\"xpaymode\":\""
                + xpaymode + '\"'
                + ",\"xbankcode\":\""
                + xbankcode + '\"'
                + ",\"xbankaccno\":\""
                + xbankaccno + '\"'
                + ",\"xaccname\":\""
                + xaccname + '\"'
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
