package com.pactera.yhl.apps.develop.premiums.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor

public class PremiumsKafkaEntity02 implements Serializable,KafkaEntity{
    public String workarea;
    public String managecom;
    public String prem;
    public String agentcom;
    public String branch_name;
    public String channel_id;
}
