package com.hjj

import java.util
import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


// 输入订单事件样例类,订单id，事件类型，交易id，事件时间
case class OrderEvent(orderId: Long,eventTypes: String, txId: String, eventTime: Long)
// 输出结果样例类，订单id，结果信息
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {

  def main(args: Array[String]): Unit = {
    // 配置kafka属性参数
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    // flink环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = env.addSource(new FlinkKafkaConsumer[String]("orderEvent", new SimpleStringSchema(), properties))
    val orderEventStream = resource.map(data => {
      val dataArray = data.split(",")
      OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
    })
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)

    // 使用cep方式
    val orderPayPattern = Pattern.begin[OrderEvent]("begin").where(_.eventTypes == "create")
      .followedBy("follow").where(_.eventTypes == "pay")
      .within(Time.seconds(2))

    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)
    // 定义存储超时的流
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")
    val resultStream = patternStream.select(orderTimeoutOutputTag, new OrderTimeoutSelect(), new OrderPaySelect())

    resultStream.print("payed") // 打印正常支付
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")  // 打印超时

    env.execute("order timeout")

  }
}

// 自定义超时事件序列处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent,OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId = map.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId,"timeout")
  }
}

// 自定义正常支付事件序列处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent,OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = map.get("follow").iterator().next().orderId
    OrderResult(payedOrderId,"payed successfully")
  }
}