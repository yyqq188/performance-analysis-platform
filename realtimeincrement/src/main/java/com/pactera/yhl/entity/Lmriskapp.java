package com.pactera.yhl.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:36
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Lmriskapp implements KLEntity {
    public String riskcode;
    public String riskver;
    public String riskname;
    public String kindcode;
    public String risktype;
    public String risktype1;
    public String risktype2;
    public String riskprop;
    public String riskperiod;
    public String risktypedetail;
    public String riskflag;
    public String poltype;
    public String investflag;
    public String bonusflag;
    public String bonusmode;
    public String listflag;
    public String subriskflag;
    public String caldigital;
    public String calchomode;
    public String riskamntmult;
    public String insuperiodflag;
    public String maxendperiod;
    public String agelmt;
    public String signdatecalmode;
    public String protocolflag;
    public String getchgflag;
    public String protocolpayflag;
    public String ensuplanflag;
    public String ensuplanadjflag;
    public String startdate;
    public String enddate;
    public String minappntage;
    public String maxappntage;
    public String maxinsuredage;
    public String mininsuredage;
    public String appinterest;
    public String apppremrate;
    public String insuredflag;
    public String shareflag;
    public String bnfflag;
    public String temppayflag;
    public String inppayplan;
    public String impartflag;
    public String insuexpeflag;
    public String loanfalg;
    public String mortagageflag;
    public String idifreturnflag;
    public String cutamntstoppay;
    public String rinsrate;
    public String saleflag;
    public String fileappflag;
    public String mngcom;
    public String autopayflag;
    public String needprinthospital;
    public String needprintget;
    public String risktype3;
    public String risktype4;
    public String risktype5;
    public String notprintpol;
    public String needgetpoldate;
    public String needrereadbank;
    public String risktype6;
    public String risktype7;
    public String risktype8;
    public String risktype9;
    public String insuredageflag;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String riskhmflag;
    public String bflag1;
    public String bflag2;
    public String bflag3;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"Lmriskapp\":{"
                + "\"riskcode\":\""
                + riskcode + '\"'
                + ",\"riskver\":\""
                + riskver + '\"'
                + ",\"riskname\":\""
                + riskname + '\"'
                + ",\"kindcode\":\""
                + kindcode + '\"'
                + ",\"risktype\":\""
                + risktype + '\"'
                + ",\"risktype1\":\""
                + risktype1 + '\"'
                + ",\"risktype2\":\""
                + risktype2 + '\"'
                + ",\"riskprop\":\""
                + riskprop + '\"'
                + ",\"riskperiod\":\""
                + riskperiod + '\"'
                + ",\"risktypedetail\":\""
                + risktypedetail + '\"'
                + ",\"riskflag\":\""
                + riskflag + '\"'
                + ",\"poltype\":\""
                + poltype + '\"'
                + ",\"investflag\":\""
                + investflag + '\"'
                + ",\"bonusflag\":\""
                + bonusflag + '\"'
                + ",\"bonusmode\":\""
                + bonusmode + '\"'
                + ",\"listflag\":\""
                + listflag + '\"'
                + ",\"subriskflag\":\""
                + subriskflag + '\"'
                + ",\"caldigital\":\""
                + caldigital + '\"'
                + ",\"calchomode\":\""
                + calchomode + '\"'
                + ",\"riskamntmult\":\""
                + riskamntmult + '\"'
                + ",\"insuperiodflag\":\""
                + insuperiodflag + '\"'
                + ",\"maxendperiod\":\""
                + maxendperiod + '\"'
                + ",\"agelmt\":\""
                + agelmt + '\"'
                + ",\"signdatecalmode\":\""
                + signdatecalmode + '\"'
                + ",\"protocolflag\":\""
                + protocolflag + '\"'
                + ",\"getchgflag\":\""
                + getchgflag + '\"'
                + ",\"protocolpayflag\":\""
                + protocolpayflag + '\"'
                + ",\"ensuplanflag\":\""
                + ensuplanflag + '\"'
                + ",\"ensuplanadjflag\":\""
                + ensuplanadjflag + '\"'
                + ",\"startdate\":\""
                + startdate + '\"'
                + ",\"enddate\":\""
                + enddate + '\"'
                + ",\"minappntage\":\""
                + minappntage + '\"'
                + ",\"maxappntage\":\""
                + maxappntage + '\"'
                + ",\"maxinsuredage\":\""
                + maxinsuredage + '\"'
                + ",\"mininsuredage\":\""
                + mininsuredage + '\"'
                + ",\"appinterest\":\""
                + appinterest + '\"'
                + ",\"apppremrate\":\""
                + apppremrate + '\"'
                + ",\"insuredflag\":\""
                + insuredflag + '\"'
                + ",\"shareflag\":\""
                + shareflag + '\"'
                + ",\"bnfflag\":\""
                + bnfflag + '\"'
                + ",\"temppayflag\":\""
                + temppayflag + '\"'
                + ",\"inppayplan\":\""
                + inppayplan + '\"'
                + ",\"impartflag\":\""
                + impartflag + '\"'
                + ",\"insuexpeflag\":\""
                + insuexpeflag + '\"'
                + ",\"loanfalg\":\""
                + loanfalg + '\"'
                + ",\"mortagageflag\":\""
                + mortagageflag + '\"'
                + ",\"idifreturnflag\":\""
                + idifreturnflag + '\"'
                + ",\"cutamntstoppay\":\""
                + cutamntstoppay + '\"'
                + ",\"rinsrate\":\""
                + rinsrate + '\"'
                + ",\"saleflag\":\""
                + saleflag + '\"'
                + ",\"fileappflag\":\""
                + fileappflag + '\"'
                + ",\"mngcom\":\""
                + mngcom + '\"'
                + ",\"autopayflag\":\""
                + autopayflag + '\"'
                + ",\"needprinthospital\":\""
                + needprinthospital + '\"'
                + ",\"needprintget\":\""
                + needprintget + '\"'
                + ",\"risktype3\":\""
                + risktype3 + '\"'
                + ",\"risktype4\":\""
                + risktype4 + '\"'
                + ",\"risktype5\":\""
                + risktype5 + '\"'
                + ",\"notprintpol\":\""
                + notprintpol + '\"'
                + ",\"needgetpoldate\":\""
                + needgetpoldate + '\"'
                + ",\"needrereadbank\":\""
                + needrereadbank + '\"'
                + ",\"risktype6\":\""
                + risktype6 + '\"'
                + ",\"risktype7\":\""
                + risktype7 + '\"'
                + ",\"risktype8\":\""
                + risktype8 + '\"'
                + ",\"risktype9\":\""
                + risktype9 + '\"'
                + ",\"insuredageflag\":\""
                + insuredageflag + '\"'
                + ",\"etl_dt\":\""
                + etl_dt + '\"'
                + ",\"etl_tm\":\""
                + etl_tm + '\"'
                + ",\"etl_fg\":\""
                + etl_fg + '\"'
                + ",\"riskhmflag\":\""
                + riskhmflag + '\"'
                + ",\"bflag1\":\""
                + bflag1 + '\"'
                + ",\"bflag2\":\""
                + bflag2 + '\"'
                + ",\"bflag3\":\""
                + bflag3 + '\"'
                + ",\"op_ts\":\""
                + op_ts + '\"'
                + ",\"current_ts\":\""
                + current_ts + '\"'
                + ",\"load_date\":\""
                + load_date + '\"'
                + "}}";

    }
}
