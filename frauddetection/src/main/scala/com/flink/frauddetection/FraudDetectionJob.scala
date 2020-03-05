package com.flink.frauddetection

import com.flink.common.entity.{Alert, Transaction}
import com.flink.common.sink.AlertSink
import com.flink.common.source.TransactionSource
import org.apache.flink.streaming.api.scala._

object FraudDetectionJob {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val transactions: DataStream[Transaction] = env
      .addSource(new TransactionSource)
      .name("transactions")

    val alerts: DataStream[Alert] = transactions
      .keyBy(transaction => transaction.getAccountId)
      .process(new FraudDetector)
      .name("fraud-detector")

    alerts
      .addSink(new AlertSink)
      .name("send-alerts")

    env.execute("Fraud Detection")

  }
}
