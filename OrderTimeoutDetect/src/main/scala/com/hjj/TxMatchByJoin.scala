package com.hjj

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

// 采用join的方式实现订单与支付匹配
object TxMatchByJoin {
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

    // 订单流数据源
    val resource = env.addSource(new FlinkKafkaConsumer[String]("orderEvent", new SimpleStringSchema(), properties))
    val orderEventStream = resource.map(data => {
      val dataArray = data.split(",")
      OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
    })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.txId)

    // 支付完成事件数据源
    val receiptResource = env.addSource(new FlinkKafkaConsumer[String]("receipt", new SimpleStringSchema(), properties))
    val receiptEventStream = receiptResource.map(data => {
      val dataArray = data.split(",")
      ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
    })
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.txId)

    val processStream = orderEventStream.intervalJoin(receiptEventStream)
      .between(Time.seconds(-5), Time.seconds(5))
      .process(new TxPayMatchByJoin())

    processStream.print()
    env.execute("tx pay match by join")
  }
}

// 自定义join函数
class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left,right))
  }
}