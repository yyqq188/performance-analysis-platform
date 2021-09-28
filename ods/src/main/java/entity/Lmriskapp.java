package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:36
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Lmriskapp implements KLEntity{
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
}
