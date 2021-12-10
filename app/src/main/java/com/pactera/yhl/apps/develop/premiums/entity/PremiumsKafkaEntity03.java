package com.pactera.yhl.apps.develop.premiums.entity;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PremiumsKafkaEntity03 {
    public String workarea;
    public String managecom;
    public String prem;
    public String agentcom;
    public String channel_id;
    public String branch_name;
    public String branch_id;
    public String class_id;
    public String branch_id_parent;
    public String branch_id_full;
    public String contplancode;
    public String riskcode;
    public String polno;
    public String payendyear;
    public String signdate;
    public String amnt;

}
