package com.flink.frauddetection

import com.flink.common.entity.{Alert, Transaction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

object FraudDetector {
  val SMALL_AMOUNT: Double = 1.00
  val LARGE_AMOUNT: Double = 500.00
  val ONE_MINUTE: Long     = 60 * 1000L
}

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert]{

  // ValueState is always scoped to the current key and is fault tolerant
  @transient private var flagState: ValueState[java.lang.Boolean] = _

  @transient private var timerState: ValueState[java.lang.Long] = _

  override def open(parameters: Configuration): Unit = {
    // ValueStateDescriptor which contains metadata about how Flink should manage the variable
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDescriptor)

    val timerStateDescriptor = new ValueStateDescriptor("timer-state", Types.LONG)
    timerState = getRuntimeContext.getState(timerStateDescriptor)
  }

  /**
   * Called for every transaction event
   * @param transaction input stream element
   * @param context set timers using the provided context
   * @param collector zero or more elements as output
   */
  override def processElement(transaction: Transaction, context: KeyedProcessFunction[Long, Transaction, Alert]#Context, collector: Collector[Alert]): Unit = {

    val lastTransactionWasSmall = flagState.value

    // If the state for a particular key is empty, such as at the beginning of an application or after calling ValueState#clear,
    // then ValueState#value will return null
    if(lastTransactionWasSmall != null) {
      if(transaction.getAmount > FraudDetector.LARGE_AMOUNT) {
        val alert = new Alert
        alert.setId(transaction.getAccountId)
        collector.collect(alert)
      }
      // cleanup state
      cleanup(context)
    }

    if(transaction.getAmount < FraudDetector.SMALL_AMOUNT) {
      // set the flag to true
      flagState.update(true)

      // set the timer and timer state
      // When a timer fires, it calls KeyedProcessFunction#onTimer
      val timer = context.timerService().currentProcessingTime() + FraudDetector.ONE_MINUTE
      context.timerService().registerProcessingTimeTimer(timer)
      timerState.update(timer)
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext, out: Collector[Alert]): Unit = {
    // remove flag after 1 minute
    timerState.clear()
    flagState.clear()
  }

  private def cleanup(ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context): Unit = {
    // delete timer
    val timer = timerState.value()
    ctx.timerService().deleteProcessingTimeTimer(timer)

    // clean up all states
    timerState.clear()
    flagState.clear()
  }

}