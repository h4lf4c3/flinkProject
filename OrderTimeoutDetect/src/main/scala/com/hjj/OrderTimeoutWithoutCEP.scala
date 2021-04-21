package com.hjj

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object OrderTimeoutWithoutCEP {
  // 定义分流outputtag
  val orderTimeOutputTag = new OutputTag[OrderResult]("orderTimeout")
  def main(args: Array[String]): Unit = {
    // 配置kafka属性参数
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    //flink环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    // val resource = env.readTextFile("D:\\Code\\javaCode\\UserBehaviorBaseFlink\\OrderTimeoutDetect\\src\\main\\resources\\OrderLog.csv")
    val resource = env.addSource(new FlinkKafkaConsumer[String]("orderEvent", new SimpleStringSchema(), properties))
    val orderEventStream = resource.map(data => {
      val dataArray = data.split(",")
      OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
    })
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)

    // 定义process进行超时监测
    val orderResultStream = orderEventStream.process(new OrderPayMatch())
    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderTimeOutputTag).print("timeout")

    env.execute("order timeout without cep")


  }
  // 自定义process函数
  class OrderPayMatch() extends KeyedProcessFunction[Long,OrderEvent,OrderResult]{
    // 定义是否支付状态
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))
    // 定义定时器状态
    lazy val timeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeState", classOf[Long]))

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      val isPayed = isPayedState.value()  // 获取状态
      val timestamp = timeState.value()

      if (value.eventTypes =="create"){
        // 如果是create事件，接下来判断pay是否来过
        if(isPayed){
          // 如果已经pay过，匹配成功，输出主流，清空状态
          out.collect(OrderResult(value.orderId,"payed"))
          ctx.timerService().deleteEventTimeTimer(timestamp)
          isPayedState.clear()
          timeState.clear()
        }else{
          // 如果没有pay过，注册定时器等待pay的到来
          val ts = value.eventTime  + 15 * 60
          ctx.timerService().registerEventTimeTimer(ts)
          timeState.update(ts)
        }
      }else if(value.eventTypes == "pay"){
        // 如果是pay事件，那么判断是否create过，用timer表示
        if (timestamp>0){
          // 如果有定时器，说明已经有create来过
          // 继续判断，是否超过了timeout时间
          if(timestamp > value.eventTime ){
            // 2.1.1 如果定时器时间还没到，那么输出成功匹配
            out.collect( OrderResult(value.orderId, "payed successfully") )
          }else{
            // 2.1.2 如果当前pay的时间已经超时，那么输出到侧输出流
            ctx.output(orderTimeOutputTag, OrderResult(value.orderId, "payed but already timeout"))
          }
          ctx.timerService().deleteEventTimeTimer(timestamp)
          isPayedState.clear()
          timeState.clear()
        }else{
          // pay先到了，更新状态，注册定时器等待create
          isPayedState.update(true)
          ctx.timerService().registerEventTimeTimer(value.eventTime*1000L)
          timeState.update(value.eventTime*1000L)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      if (isPayedState.value()){
        // 如果为true，那么pay先到，没等到create
        ctx.output(orderTimeOutputTag,OrderResult(ctx.getCurrentKey,"already pay but not found create"))
      } else{
        // 等到了create，但是没等到pay
        ctx.output(orderTimeOutputTag,OrderResult(ctx.getCurrentKey,"order timeout"))
      }
      isPayedState.clear()
      timeState.clear()
    }
  }
}
