package com.pactera.yhl.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:08
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Lcaddress implements KLEntity {
    public String customerno;
    public String addressno;
    public String postaladdress;
    public String zipcode;
    public String phone;
    public String fax;
    public String homeaddress;
    public String homezipcode;
    public String homephone;
    public String homefax;
    public String companyaddress;
    public String companyzipcode;
    public String companyphone;
    public String companyfax;
    public String mobile;
    public String mobilechs;
    public String email;
    public String bp;
    public String mobile2;
    public String mobilechs2;
    public String email2;
    public String bp2;
    public String operator;
    public String makedate;
    public String maketime;
    public String modifydate;
    public String modifytime;
    public String grpname;
    public String province;
    public String city;
    public String county;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"Lcaddress\":{"
                + "\"customerno\":\""
                + customerno + '\"'
                + ",\"addressno\":\""
                + addressno + '\"'
                + ",\"postaladdress\":\""
                + postaladdress + '\"'
                + ",\"zipcode\":\""
                + zipcode + '\"'
                + ",\"phone\":\""
                + phone + '\"'
                + ",\"fax\":\""
                + fax + '\"'
                + ",\"homeaddress\":\""
                + homeaddress + '\"'
                + ",\"homezipcode\":\""
                + homezipcode + '\"'
                + ",\"homephone\":\""
                + homephone + '\"'
                + ",\"homefax\":\""
                + homefax + '\"'
                + ",\"companyaddress\":\""
                + companyaddress + '\"'
                + ",\"companyzipcode\":\""
                + companyzipcode + '\"'
                + ",\"companyphone\":\""
                + companyphone + '\"'
                + ",\"companyfax\":\""
                + companyfax + '\"'
                + ",\"mobile\":\""
                + mobile + '\"'
                + ",\"mobilechs\":\""
                + mobilechs + '\"'
                + ",\"email\":\""
                + email + '\"'
                + ",\"bp\":\""
                + bp + '\"'
                + ",\"mobile2\":\""
                + mobile2 + '\"'
                + ",\"mobilechs2\":\""
                + mobilechs2 + '\"'
                + ",\"email2\":\""
                + email2 + '\"'
                + ",\"bp2\":\""
                + bp2 + '\"'
                + ",\"operator\":\""
                + operator + '\"'
                + ",\"makedate\":\""
                + makedate + '\"'
                + ",\"maketime\":\""
                + maketime + '\"'
                + ",\"modifydate\":\""
                + modifydate + '\"'
                + ",\"modifytime\":\""
                + modifytime + '\"'
                + ",\"grpname\":\""
                + grpname + '\"'
                + ",\"province\":\""
                + province + '\"'
                + ",\"city\":\""
                + city + '\"'
                + ",\"county\":\""
                + county + '\"'
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
