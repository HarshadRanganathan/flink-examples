package com.flink.frauddetection

import com.flink.common.entity.{Alert, Transaction}
import com.flink.common.sink.AlertSink
import com.flink.common.source.TransactionSource
import org.apache.flink.streaming.api.scala._

object FraudDetectionJob {

  def main(args: Array[String]): Unit = {
    // execution environment is how you set properties for your Job, create your sources, and finally trigger the execution of the Job
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // configure the source to ingest data from
    val transactions: DataStream[Transaction] = env
      .addSource(new TransactionSource)
      .name("transactions")

    /**
     * To detect frauds on a per account basis, we partition the data by account id so that
     * same physical task processes all records for a particular key
     * Also, operator immediately after a keyBy is executed within a keyed context
     */
    val alerts: DataStream[Alert] = transactions
      .keyBy(transaction => transaction.getAccountId)
      .process(new FraudDetector)
      .name("fraud-detector")

    // write to sink
    alerts
      .addSink(new AlertSink)
      .name("send-alerts")

    env.execute("Fraud Detection")

  }
}
