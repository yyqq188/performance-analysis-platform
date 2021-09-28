package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:54
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Ljagetendorse {
    public String actugetno;
    public String endorsementno;
    public String feeoperationtype;
    public String feefinatype;
    public String grpcontno;
    public String contno;
    public String grppolno;
    public String polno;
    public String otherno;
    public String othernotype;
    public String dutycode;
    public String payplancode;
    public String appntno;
    public String insuredno;
    public String getnoticeno;
    public String getdate;
    public String enteraccdate;
    public String getconfirmdate;
    public BigDecimal getmoney;
    public String kindcode;
    public String riskcode;
    public String riskversion;
    public String managecom;
    public String agentcom;
    public String agenttype;
    public String agentcode;
    public String agentgroup;
    public String grpname;
    public String handler;
    public String approvedate;
    public String approvetime;
    public String poltype;
    public String approvecode;
    public String operator;
    public String serialno;
    public String modifydate;
    public String makedate;
    public String maketime;
    public String getflag;
    public String modifytime;
    public String finstate;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;
}
