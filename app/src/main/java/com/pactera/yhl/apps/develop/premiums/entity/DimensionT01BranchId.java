package com.pactera.yhl.apps.develop.premiums.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
/**
 * 暂时没用
 */
public class DimensionT01BranchId implements Serializable,KafkaEntity{
    public String branch_id;
    public String branch_id_parent;
    public String branch_id_full;
    public String branch_name;
}
