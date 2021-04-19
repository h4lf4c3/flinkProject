package com.hjj


import java.util
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object LoginEventCEP {
  def main(args: Array[String]): Unit = {
    // 配置kafka属性参数
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // Flink环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 数据源
//    val resource = env.readTextFile("D:\\Code\\javaCode\\UserBehaviorBaseFlink\\LoginFailDetect\\src\\main\\resources\\LoginEvent.log"
    val resource = env.addSource(new FlinkKafkaConsumer[String]("loginEvent", new SimpleStringSchema(), properties))
    val loginEventStream = resource.map( data => {
      val dataArray = data.split(",")
      LoginEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
      override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
    })
      .keyBy(_.userId)

    // 定义匹配模式
    val loginEventPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(3))

    // 在事件流上应用模式，得到pattern stream
    val patternStream = CEP.pattern(loginEventStream, loginEventPattern)
    // 从pattern stream上应用select function，然后监测出事件序列
    val loginFailDataStream = patternStream.select(new LoginFailMatch())
    loginFailDataStream.print()
    env.execute("login fial with cep")
  }
}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent,Warning]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    val firstFail = map.get("begin").iterator().next()
    val lastFail = map.get("next").iterator().next()
    Warning(firstFail.userId,firstFail.eventTime,lastFail.eventTime,"login fail")
  }
}
