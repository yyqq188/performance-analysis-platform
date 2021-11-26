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
    public String edorvalidate; //lpedoritem
    public String edortype;  //lpedoritem
    public String edorstate; //lpedoritem
    public String workarea; //salesinfo
//    public String area_type; //salesinfo

}
