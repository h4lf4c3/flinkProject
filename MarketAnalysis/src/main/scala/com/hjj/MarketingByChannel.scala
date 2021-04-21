package com.hjj

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.util.Random

/*
自定义source实现用户渠道分析
 */

// 用户app行为样例类  用户id，用户行为，用户渠道，时间戳
case class MarketingUserBehavior( userId: String, behavior: String, channel: String, timestamp: Long)
// 输出结果样例类 开始窗口，结束窗口，用户渠道，用户行为，计数
case class MarketingViewCount( windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

object MarketingByChannel {
  def main(args: Array[String]): Unit = {

    // flink环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 数据源，采用随机生成的方式，自定义source
    val dataStream = env.addSource( new SimulatedEventSource() )
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "uninstall")
      .map(data => {
        ((data.channel,data.behavior),1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1),Time.seconds(10))
      .process(new MarketingCountByChannel())

    dataStream.print()
    env.execute("app marketing by channel")

  }
}

/*
生成随机数据类，自定义source
 */
class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior]{
  var running = true
  // 定义用户行为种类
  val behaviorTypes = Seq("click", "download", "install", "uninstall")
  // 定义渠道种类
  val channelTypes = Seq("wechat", "weibo", "xiaomistore", "appstore", "huaweistore")
  val random = new Random()

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    val maxElements = Long.MaxValue
    var count = 0L
    // 只要在运行，并且计数小于最大就一直循环，相当于死循环
    while (running && count < maxElements){
      val userId = UUID.randomUUID().toString
      val behavior = behaviorTypes(random.nextInt(behaviorTypes.size))
      val channel = channelTypes(random.nextInt(channelTypes.size))
      val timestamp = System.currentTimeMillis()

      ctx.collect(MarketingUserBehavior(userId,behavior,channel,timestamp))
      count += 1
      TimeUnit.MILLISECONDS.sleep(10L) // 每次循环休息1秒
    }
  }
  override def cancel(): Unit = running = false
}

/*
自定义process窗口类
 */
class MarketingCountByChannel() extends ProcessWindowFunction[((String,String),Long),MarketingViewCount,(String,String),TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    val startTimestamp = new Timestamp(context.window.getStart).toString // 获取窗口开始时间
    val endTimestamp = new Timestamp(context.window.getEnd).toString // 获取窗口结束时间
    val channel = key._1
    val behavior = key._2
    val count = elements.size
    out.collect(MarketingViewCount(startTimestamp,endTimestamp,channel,behavior,count))
  }
}