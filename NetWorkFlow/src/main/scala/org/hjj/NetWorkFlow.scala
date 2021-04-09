package org.hjj

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._


case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

case class UrlViewCount(url: String, windowEnd: Long, count: Long)


object NetWorkFlow {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.socketTextStream("localhost",8888)
      .map(data => {
        val dataArray = data.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(dataArray(3).trim).getTime
        ApacheLogEvent(dataArray(0).trim,dataArray(1).trim,timestamp,dataArray(5).trim,dataArray(6).trim)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10),Time.seconds(5))
      .allowedLateness(Time.seconds(60))
      .aggregate(new CountAgg(),new WindowResult())

    val processStream = dataStream
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))

    dataStream.print("agg")
    processStream.print("process")

    env.execute("network flow")

  }
}


class CountAgg() extends AggregateFunction[ApacheLogEvent,Long,Long]{
  override def createAccumulator(): Long = ???

  override def add(in: ApacheLogEvent, acc: Long): Long = ???

  override def getResult(acc: Long): Long = ???

  override def merge(acc: Long, acc1: Long): Long = ???
}