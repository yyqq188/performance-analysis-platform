package com.pactera.yhl.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:42
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Lcphoinfonewresult implements KLEntity {
    public String contno;
    public String state;
    public String sendstate;
    public String senddate;
    public String standbyflag1;
    public String standbyflag2;
    public String makedate;
    public String maketime;
    public String modifydate;
    public String modifytime;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"Lcphoinfonewresult\":{"
                + "\"contno\":\""
                + contno + '\"'
                + ",\"state\":\""
                + state + '\"'
                + ",\"sendstate\":\""
                + sendstate + '\"'
                + ",\"senddate\":\""
                + senddate + '\"'
                + ",\"standbyflag1\":\""
                + standbyflag1 + '\"'
                + ",\"standbyflag2\":\""
                + standbyflag2 + '\"'
                + ",\"makedate\":\""
                + makedate + '\"'
                + ",\"maketime\":\""
                + maketime + '\"'
                + ",\"modifydate\":\""
                + modifydate + '\"'
                + ",\"modifytime\":\""
                + modifytime + '\"'
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
