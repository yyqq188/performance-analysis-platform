package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:48
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Lktransstatus implements KLEntity{
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
}
