package com.pactera.yhl.apps.develop.premiums.entity;

import lombok.Data;

import javax.annotation.Nullable;
import java.io.Serializable;

@Data
public class LbpolKafka02 implements Serializable,KafkaEntity{
    public String agentcode; //lbpol
    public String managecom; //lbpol    lpedoritem
    public String prem; //lbpol
    public String agentcom; //lbpol
    public String modifydate; //lpedoritem
    public String edortype;  //lpedoritem
    public String edorstate; //lpedoritem
    public String workarea; //salesinfo

    public String edorno;

    public String polno;
    public String payyears;
    public String signdate;
    public String amnt;

    public String channel_id;
}
