package com.flink.spendreport

import com.flink.common.table.{BoundedTransactionTableSource, SpendReportTableSink, TruncateDateToHour}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment

object SpendReportJob {

  def main(args: Array[String]): Unit = {
    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv:BatchTableEnvironment = BatchTableEnvironment.create(env)

    /* TODO: uses deprecated methods */
    tEnv.registerTableSource("transactions", new BoundedTransactionTableSource)
    tEnv.registerTableSink("spend_report", new SpendReportTableSink)

    tEnv.registerFunction("truncateDateToHour", new TruncateDateToHour())

    /* TODO: uses deprecated methods */
    tEnv
      .scan("transactions")
      .select("accountId, timestamp.truncateDateToHour as timestamp, amount")
      .groupBy("accountId, timestamp")
      .select("accountId, timestamp, amount.sum as total")
      .insertInto("spend_report")

    env.execute("Spend Report")

  }
}
