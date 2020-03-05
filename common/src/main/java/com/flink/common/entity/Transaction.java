package com.flink.common.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Transaction {
    private long accountId;

    private long timestamp;

    private double amount;
}
