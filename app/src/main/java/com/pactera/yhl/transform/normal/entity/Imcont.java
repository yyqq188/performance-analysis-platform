package com.pactera.yhl.transform.normal.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:39
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Imcont implements KLEntity {
    public String data_source;
    public String branch_no;
    public String sales_id;
    public String plcid;
    public String apladdr;
    public String aplname;
    public String aplphone;
    public String outbid;
    public String pay_kind;
    public String bankagentid;
    public String aplidno;
    public String hobproflag;
    public String askgrpcontno;
    public String dutycount;
    public String agentcodeisnet;
    public String agentcomisnet;
    public String policyflag;
    public String amnt;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"Imcont\":{"
                + "\"data_source\":\""
                + data_source + '\"'
                + ",\"branch_no\":\""
                + branch_no + '\"'
                + ",\"sales_id\":\""
                + sales_id + '\"'
                + ",\"plcid\":\""
                + plcid + '\"'
                + ",\"apladdr\":\""
                + apladdr + '\"'
                + ",\"aplname\":\""
                + aplname + '\"'
                + ",\"aplphone\":\""
                + aplphone + '\"'
                + ",\"outbid\":\""
                + outbid + '\"'
                + ",\"pay_kind\":\""
                + pay_kind + '\"'
                + ",\"bankagentid\":\""
                + bankagentid + '\"'
                + ",\"aplidno\":\""
                + aplidno + '\"'
                + ",\"hobproflag\":\""
                + hobproflag + '\"'
                + ",\"askgrpcontno\":\""
                + askgrpcontno + '\"'
                + ",\"dutycount\":\""
                + dutycount + '\"'
                + ",\"agentcodeisnet\":\""
                + agentcodeisnet + '\"'
                + ",\"agentcomisnet\":\""
                + agentcomisnet + '\"'
                + ",\"policyflag\":\""
                + policyflag + '\"'
                + ",\"amnt\":\""
                + amnt + '\"'
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
