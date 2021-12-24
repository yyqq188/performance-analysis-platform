package com.pactera.yhl.transform.normal.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:44
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Ljapayperson implements KLEntity{
    public String polno;
    public Integer paycount;
    public String grpcontno;
    public String grppolno;
    public String contno;
    public String managecom;
    public String agentcom;
    public String agenttype;
    public String riskcode;
    public String agentcode;
    public String agentgroup;
    public String paytypeflag;
    public String appntno;
    public String payno;
    public String payaimclass;
    public String dutycode;
    public String payplancode;
    public String sumduepaymoney;
    public String sumactupaymoney;
    public Integer payintv;
    public String paydate;
    public String paytype;
    public String enteraccdate;
    public String confdate;
    public String lastpaytodate;
    public String curpaytodate;
    public String ininsuaccstate;
    public String approvecode;
    public String approvedate;
    public String approvetime;
    public String serialno;
    public String operator;
    public String makedate;
    public String maketime;
    public String getnoticeno;
    public String modifydate;
    public String modifytime;
    public String finstate;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"Ljapayperson\":{"
                + "\"polno\":\""
                + polno + '\"'
                + ",\"paycount\":"
                + paycount
                + ",\"grpcontno\":\""
                + grpcontno + '\"'
                + ",\"grppolno\":\""
                + grppolno + '\"'
                + ",\"contno\":\""
                + contno + '\"'
                + ",\"managecom\":\""
                + managecom + '\"'
                + ",\"agentcom\":\""
                + agentcom + '\"'
                + ",\"agenttype\":\""
                + agenttype + '\"'
                + ",\"riskcode\":\""
                + riskcode + '\"'
                + ",\"agentcode\":\""
                + agentcode + '\"'
                + ",\"agentgroup\":\""
                + agentgroup + '\"'
                + ",\"paytypeflag\":\""
                + paytypeflag + '\"'
                + ",\"appntno\":\""
                + appntno + '\"'
                + ",\"payno\":\""
                + payno + '\"'
                + ",\"payaimclass\":\""
                + payaimclass + '\"'
                + ",\"dutycode\":\""
                + dutycode + '\"'
                + ",\"payplancode\":\""
                + payplancode + '\"'
                + ",\"sumduepaymoney\":\""
                + sumduepaymoney + '\"'
                + ",\"sumactupaymoney\":\""
                + sumactupaymoney + '\"'
                + ",\"payintv\":"
                + payintv
                + ",\"paydate\":\""
                + paydate + '\"'
                + ",\"paytype\":\""
                + paytype + '\"'
                + ",\"enteraccdate\":\""
                + enteraccdate + '\"'
                + ",\"confdate\":\""
                + confdate + '\"'
                + ",\"lastpaytodate\":\""
                + lastpaytodate + '\"'
                + ",\"curpaytodate\":\""
                + curpaytodate + '\"'
                + ",\"ininsuaccstate\":\""
                + ininsuaccstate + '\"'
                + ",\"approvecode\":\""
                + approvecode + '\"'
                + ",\"approvedate\":\""
                + approvedate + '\"'
                + ",\"approvetime\":\""
                + approvetime + '\"'
                + ",\"serialno\":\""
                + serialno + '\"'
                + ",\"operator\":\""
                + operator + '\"'
                + ",\"makedate\":\""
                + makedate + '\"'
                + ",\"maketime\":\""
                + maketime + '\"'
                + ",\"getnoticeno\":\""
                + getnoticeno + '\"'
                + ",\"modifydate\":\""
                + modifydate + '\"'
                + ",\"modifytime\":\""
                + modifytime + '\"'
                + ",\"finstate\":\""
                + finstate + '\"'
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
