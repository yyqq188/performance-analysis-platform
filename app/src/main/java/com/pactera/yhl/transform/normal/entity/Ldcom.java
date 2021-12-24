package com.pactera.yhl.transform.normal.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:56
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Ldcom implements KLEntity {
    public String comcode;
    public String outcomcode;
    public String name;
    public String shortname;
    public String address;
    public String zipcode;
    public String phone;
    public String fax;
    public String email;
    public String webaddress;
    public String satrapname;
    public String insumonitorcode;
    public String insureid;
    public String signid;
    public String regionalismcode;
    public String comnature;
    public String validcode;
    public String sign;
    public String comcitysize;
    public String servicename;
    public String serviceno;
    public String servicephone;
    public String servicepostaddress;
    public String servicepostzipcode;
    public String letterservicename;
    public String letterserviceno;
    public String letterservicephone;
    public String letterservicepostaddress;
    public String letterservicepostzipcode;
    public String comgrade;
    public String comareatype;
    public String innercomname;
    public String salecomname;
    public String taxregistryno;
    public String showname;
    public String servicephone2;
    public String claimreportphone;
    public String peorderphone;
    public String backupaddress1;
    public String backupaddress2;
    public String backupphone1;
    public String backupphone2;
    public String ename;
    public String eshortname;
    public String eaddress;
    public String eservicepostaddress;
    public String eletterservicename;
    public String eletterservicepostaddress;
    public String ebackupaddress1;
    public String ebackupaddress2;
    public String servicephone1;
    public String printcomname;
    public String supercomcode;
    public String operator;
    public String makedate;
    public String maketime;
    public String modifydate;
    public String modifytime;
    public String crs_check_status;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"Ldcom\":{"
                + "\"comcode\":\""
                + comcode + '\"'
                + ",\"outcomcode\":\""
                + outcomcode + '\"'
                + ",\"name\":\""
                + name + '\"'
                + ",\"shortname\":\""
                + shortname + '\"'
                + ",\"address\":\""
                + address + '\"'
                + ",\"zipcode\":\""
                + zipcode + '\"'
                + ",\"phone\":\""
                + phone + '\"'
                + ",\"fax\":\""
                + fax + '\"'
                + ",\"email\":\""
                + email + '\"'
                + ",\"webaddress\":\""
                + webaddress + '\"'
                + ",\"satrapname\":\""
                + satrapname + '\"'
                + ",\"insumonitorcode\":\""
                + insumonitorcode + '\"'
                + ",\"insureid\":\""
                + insureid + '\"'
                + ",\"signid\":\""
                + signid + '\"'
                + ",\"regionalismcode\":\""
                + regionalismcode + '\"'
                + ",\"comnature\":\""
                + comnature + '\"'
                + ",\"validcode\":\""
                + validcode + '\"'
                + ",\"sign\":\""
                + sign + '\"'
                + ",\"comcitysize\":\""
                + comcitysize + '\"'
                + ",\"servicename\":\""
                + servicename + '\"'
                + ",\"serviceno\":\""
                + serviceno + '\"'
                + ",\"servicephone\":\""
                + servicephone + '\"'
                + ",\"servicepostaddress\":\""
                + servicepostaddress + '\"'
                + ",\"servicepostzipcode\":\""
                + servicepostzipcode + '\"'
                + ",\"letterservicename\":\""
                + letterservicename + '\"'
                + ",\"letterserviceno\":\""
                + letterserviceno + '\"'
                + ",\"letterservicephone\":\""
                + letterservicephone + '\"'
                + ",\"letterservicepostaddress\":\""
                + letterservicepostaddress + '\"'
                + ",\"letterservicepostzipcode\":\""
                + letterservicepostzipcode + '\"'
                + ",\"comgrade\":\""
                + comgrade + '\"'
                + ",\"comareatype\":\""
                + comareatype + '\"'
                + ",\"innercomname\":\""
                + innercomname + '\"'
                + ",\"salecomname\":\""
                + salecomname + '\"'
                + ",\"taxregistryno\":\""
                + taxregistryno + '\"'
                + ",\"showname\":\""
                + showname + '\"'
                + ",\"servicephone2\":\""
                + servicephone2 + '\"'
                + ",\"claimreportphone\":\""
                + claimreportphone + '\"'
                + ",\"peorderphone\":\""
                + peorderphone + '\"'
                + ",\"backupaddress1\":\""
                + backupaddress1 + '\"'
                + ",\"backupaddress2\":\""
                + backupaddress2 + '\"'
                + ",\"backupphone1\":\""
                + backupphone1 + '\"'
                + ",\"backupphone2\":\""
                + backupphone2 + '\"'
                + ",\"ename\":\""
                + ename + '\"'
                + ",\"eshortname\":\""
                + eshortname + '\"'
                + ",\"eaddress\":\""
                + eaddress + '\"'
                + ",\"eservicepostaddress\":\""
                + eservicepostaddress + '\"'
                + ",\"eletterservicename\":\""
                + eletterservicename + '\"'
                + ",\"eletterservicepostaddress\":\""
                + eletterservicepostaddress + '\"'
                + ",\"ebackupaddress1\":\""
                + ebackupaddress1 + '\"'
                + ",\"ebackupaddress2\":\""
                + ebackupaddress2 + '\"'
                + ",\"servicephone1\":\""
                + servicephone1 + '\"'
                + ",\"printcomname\":\""
                + printcomname + '\"'
                + ",\"supercomcode\":\""
                + supercomcode + '\"'
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
                + ",\"crs_check_status\":\""
                + crs_check_status + '\"'
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
