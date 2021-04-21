package com.hjj

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/*
* 交易匹配监测
 */
// 定义接受事件样例类 交易id，支付渠道，事件时间
case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)

object TxMatchDetect {
  // 定义分流tag
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")
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

    // 将两条流链接处理
    val processedStream = orderEventStream.connect(receiptEventStream)
      .process(new TxPayMatch())

    processedStream.print("matched")
    processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")

    env.execute("tx match")
  }
  // 自定义process函数，实现CoProcess可以对两个流进行操作
  class TxPayMatch() extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
    // 定义状态保存到达的订单和支付完成的事件
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent]))
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))
    // 订单支付事件数据处理
    override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val receipt = receiptState.value() // 判断有没有对应的支付完成事件
      if (receipt != null){
        // 如果有receipt，则在主流中输出匹配信息，清空状态
        out.collect((value,receipt))
        receiptState.clear()
      } else {
        // 如果没有，那么把pay放入状态，并且注册定时器等待
        payState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime+5000L)
      }
    }

    // 支付成功事件处理
    override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val pay = payState.value()
      if (pay !=null){
        out.collect((pay,value))
        payState.clear()
      } else {
        receiptState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime+5000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      if (payState.value() != null){
        // receipt没来，输出pay到分流中
        ctx.output(unmatchedPays,payState.value())
      }
      if (receiptState.value() != null){
        ctx.output(unmatchedReceipts,receiptState.value())
      }
      payState.clear()
      receiptState.clear()
    }
  }
}
