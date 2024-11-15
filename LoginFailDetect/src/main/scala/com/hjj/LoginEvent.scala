package com.hjj

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

// 定义登录事件样例类，用户id，ip，事件类型，事件时间
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)
// 定义输出结果样例类，用户id，第一次失败时间，最终失败时间，警告信息
case class Warning(userId: Long, firstFailTime: String, lastFailTime: String, waringMsg: String)
object LoginEvent {
  def main(args: Array[String]): Unit = {
    // 配置kafka属性参数
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    // 定义flink环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 数据源
//    val resource = env.readTextFile("D:\\Code\\javaCode\\UserBehaviorBaseFlink\\LoginFailDetect\\src\\main\\resources\\LoginEvent.log")
    val resource = env.addSource(new FlinkKafkaConsumer[String]("loginEvent", new SimpleStringSchema(), properties))
    val loginEventStream = resource.map( data => {
      val dataArray = data.split(",")
      LoginEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
    } )
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      } )

    val waringStream = loginEventStream.keyBy(_.userId)
      .process(new LoginWaring(2))

    waringStream.print()
    env.execute("login fail detect")
  }

  class LoginWaring(maxFailTimes: Int) extends KeyedProcessFunction[Long,LoginEvent,Warning]{
    // 定义状态，保存2秒内的所有登录失败event
    lazy val loginFailState = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state",classOf[LoginEvent]))
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
      if (value.eventType == "fail") {
        val iter = loginFailState.get().iterator()
        if (iter.hasNext){
          // 如果已经有登录失败事件，那么就比较两次失败事件的时间
          val firstFail = iter.next()
          if(value.eventTime< firstFail.eventTime+2L){
            out.collect(Warning(value.userId,sdf.format(new Date(firstFail.eventTime*1000L)),sdf.format(new Date(value.eventTime*1000L)),"login fail in 2 seconds"))
          }
          loginFailState.clear()
          loginFailState.add(value)
        } else{
          // 如果是第一次登录失败，则直接添加到状态中
          loginFailState.add(value)
        }
      } else{
        // 如果登录是成功，则清空状态
        loginFailState.clear()
      }
    }
  }
}
