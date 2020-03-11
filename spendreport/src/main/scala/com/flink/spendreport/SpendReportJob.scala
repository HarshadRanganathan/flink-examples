package com.flink.spendreport

import com.flink.common.table.{SpendReportTableSink, UnboundedTransactionTableSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Tumble
import org.apache.flink.table.api.scala.StreamTableEnvironment

object SpendReportJob {

  def main(args: Array[String]): Unit = {

    /* Code block for batch processing */
//    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
//    val tEnv:BatchTableEnvironment = BatchTableEnvironment.create(env)
//    tEnv.registerTableSource("transactions", new BoundedTransactionTableSource)
//    tEnv.registerTableSink("spend_report", new SpendReportTableSink)

    /* Uses UDF function for truncating timestamp and grouping account expenditure over a period of one hour */
//    tEnv.registerFunction("truncateDateToHour", new TruncateDateToHour())
//    tEnv
//      .scan("transactions")
//      .select("accountId, timestamp.truncateDateToHour as timestamp, amount")
//      .groupBy("accountId, timestamp")
//      .select("accountId, timestamp, amount.sum as total")
//      .insertInto("spend_report")

    /* Code block for stream processing */
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tEnv:StreamTableEnvironment = StreamTableEnvironment.create(env)

    tEnv.registerTableSource("transactions", new UnboundedTransactionTableSource())
    tEnv.registerTableSink("spend_report", new SpendReportTableSink())

    /* One hour tumbling windows based on the timestamp column */
    tEnv
      .scan("transactions")
      .window(Tumble.over("1.hour").on("timestamp").as("w"))
      .groupBy("accountId, w")
      .select("accountId, w.start as timestamp, amount.sum as total")
      .insertInto("spend_report")

    env.execute("Spend Report")

  }
}
