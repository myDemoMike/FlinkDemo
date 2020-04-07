package com.my.base

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 订单支付实时监控
  *
  * @param orderId
  * @param eventType
  * @param eventTime
  */
// 输入订单事件数据流
case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

// 输出订单处理结果数据流
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读入订单数据
    val orderEventStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "other", 1558430845),
      OrderEvent(2, "pay", 1558430850),
      OrderEvent(1, "pay", 1558431920)
    ))
      .assignAscendingTimestamps(_.eventTime * 1000)

    // 定义一个带时间限制的pattern，选出先创建订单、之后又支付的事件流
    val orderPayPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("follow")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 定义一个输出标签，用来标明侧输出流
    val orderTimeoutOutputTag = OutputTag[OrderResult]("orderTimeout")

    // 将pattern作用到input stream上，得到一个pattern stream
    val patternStream = CEP.pattern(orderEventStream.keyBy(_.orderId), orderPayPattern)

    import scala.collection.Map
    // 调用select得到最后的复合输出流
    val complexResult: DataStream[OrderResult] = patternStream.select(orderTimeoutOutputTag)(
      // pattern timeout function
      (orderPayEvents: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val timeoutOrderId = orderPayEvents.getOrElse("begin", null).iterator.next().orderId
        OrderResult(timeoutOrderId, "order time out")
      }
    )(
      // pattern select function
      (orderPayEvents: Map[String, Iterable[OrderEvent]]) => {
        val payedOrderId = orderPayEvents.getOrElse("follow", null).iterator.next().orderId
        OrderResult(payedOrderId, "order payed successfully")
      }
    )

    // 已正常支付的数据流
    complexResult.print("payed")

    // 从复合输出流里拿到侧输出流
    val timeoutResult = complexResult.getSideOutput(orderTimeoutOutputTag)
    timeoutResult.print("timeout")

    env.execute("Order Timeout Detect")
  }
}
