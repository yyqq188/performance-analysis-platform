package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:32
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Ldplan {
    public String contplancode;
    public String contplanname;
    public String plantype;
    public String planrule;
    public String plansql;
    public String remark;
    public String operator;
    public String makedate;
    public String maketime;
    public String modifydate;
    public String modifytime;
    public Integer peoples3;
    public String managecom;
    public String salechnl;
    public String startdate;
    public String enddate;
    public String contplancode2;
    public String contplanname2;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;
}
