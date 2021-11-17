package com.pactera.yhl.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:48
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Lktransstatus implements KLEntity {
    public String transcode;
    public String reportno;
    public String bankcode;
    public String bankbranch;
    public String banknode;
    public String bankoperator;
    public String transno;
    public String funcflag;
    public String transdate;
    public String transtime;
    public String managecom;
    public String riskcode;
    public String proposalno;
    public String prtno;
    public String polno;
    public String edorno;
    public String tempfeeno;
    public String transamnt;
    public String bankacc;
    public String rcode;
    public String transstatus;
    public String status;
    public String descr;
    public String temp;
    public String makedate;
    public String maketime;
    public String modifydate;
    public String modifytime;
    public String state_code;
    public String requestid;
    public String outservicecode;
    public String clientip;
    public String clientport;
    public String issueway;
    public String servicestarttime;
    public String serviceendtime;
    public String rbankvsmp;
    public String desbankvsmp;
    public String rmpvskernel;
    public String desmpvskernel;
    public String resultbalance;
    public String desbalance;
    public String bak1;
    public String bak2;
    public String bak3;
    public String bak4;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"Lktransstatus\":{"
                + "\"transcode\":\""
                + transcode + '\"'
                + ",\"reportno\":\""
                + reportno + '\"'
                + ",\"bankcode\":\""
                + bankcode + '\"'
                + ",\"bankbranch\":\""
                + bankbranch + '\"'
                + ",\"banknode\":\""
                + banknode + '\"'
                + ",\"bankoperator\":\""
                + bankoperator + '\"'
                + ",\"transno\":\""
                + transno + '\"'
                + ",\"funcflag\":\""
                + funcflag + '\"'
                + ",\"transdate\":\""
                + transdate + '\"'
                + ",\"transtime\":\""
                + transtime + '\"'
                + ",\"managecom\":\""
                + managecom + '\"'
                + ",\"riskcode\":\""
                + riskcode + '\"'
                + ",\"proposalno\":\""
                + proposalno + '\"'
                + ",\"prtno\":\""
                + prtno + '\"'
                + ",\"polno\":\""
                + polno + '\"'
                + ",\"edorno\":\""
                + edorno + '\"'
                + ",\"tempfeeno\":\""
                + tempfeeno + '\"'
                + ",\"transamnt\":\""
                + transamnt + '\"'
                + ",\"bankacc\":\""
                + bankacc + '\"'
                + ",\"rcode\":\""
                + rcode + '\"'
                + ",\"transstatus\":\""
                + transstatus + '\"'
                + ",\"status\":\""
                + status + '\"'
                + ",\"descr\":\""
                + descr + '\"'
                + ",\"temp\":\""
                + temp + '\"'
                + ",\"makedate\":\""
                + makedate + '\"'
                + ",\"maketime\":\""
                + maketime + '\"'
                + ",\"modifydate\":\""
                + modifydate + '\"'
                + ",\"modifytime\":\""
                + modifytime + '\"'
                + ",\"state_code\":\""
                + state_code + '\"'
                + ",\"requestid\":\""
                + requestid + '\"'
                + ",\"outservicecode\":\""
                + outservicecode + '\"'
                + ",\"clientip\":\""
                + clientip + '\"'
                + ",\"clientport\":\""
                + clientport + '\"'
                + ",\"issueway\":\""
                + issueway + '\"'
                + ",\"servicestarttime\":\""
                + servicestarttime + '\"'
                + ",\"serviceendtime\":\""
                + serviceendtime + '\"'
                + ",\"rbankvsmp\":\""
                + rbankvsmp + '\"'
                + ",\"desbankvsmp\":\""
                + desbankvsmp + '\"'
                + ",\"rmpvskernel\":\""
                + rmpvskernel + '\"'
                + ",\"desmpvskernel\":\""
                + desmpvskernel + '\"'
                + ",\"resultbalance\":\""
                + resultbalance + '\"'
                + ",\"desbalance\":\""
                + desbalance + '\"'
                + ",\"bak1\":\""
                + bak1 + '\"'
                + ",\"bak2\":\""
                + bak2 + '\"'
                + ",\"bak3\":\""
                + bak3 + '\"'
                + ",\"bak4\":\""
                + bak4 + '\"'
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
                + ",\"load_date\":\""
                + load_date + '\"'
                + "}}";

    }
}
