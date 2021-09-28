package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:54
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Lmedoritem {
    public String edorcode;
    public String edorname;
    public String appobj;
    public String displayflag;
    public String calflag;
    public String needdetail;
    public String grpneedlist;
    public String edorpopedom;
    public String edortypeflag;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;
}
