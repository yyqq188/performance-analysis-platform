package com.pactera.yhl.apps.develop.premiums.entity;

import lombok.Data;

import java.io.Serializable;

@Data
public class LbpolKafka03 implements Serializable,KafkaEntity{
    public String agentcode; //lbpol
    public String managecom; //lbpol    lpedoritem
    public String prem; //lbpol
    public String agentcom; //lbpol
    public String modifydate; //lpedoritem
    public String edortype;  //lpedoritem
    public String edorstate; //lpedoritem
    public String workarea; //salesinfo
    public String branch_id; //branchinfo
    public String branch_name;//branchinfo

    public String edorno;

    public String polno;
    public String payyears;
    public String signdate;
    public String amnt;

    public String channel_id;
    public String class_id;
    public String branch_id_parent;
    public String branch_id_full;
    public String contno;
    public String payintv;
    public String payendyear;
    public String insuyear;
    public String mainpolno;

    public String contplancode;
    public String riskcode;
}
