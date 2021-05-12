package com.hjj

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/*
广告统计
 */
// 广告点击样例类，用户id，广告id，省份，城市，时间戳
case class AdClickEvent( userId: String, adId: Long, province: String, city: String, timestamp: Long)
// 省份统计结果样例类
case class CountByProvince( windowEnd: String, province: String, count: Long)
// 黑名单结果信息样例类
case class BlackListWarning( userId: Long, adId: Long, msg: String)

object AdStatic {
  val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")
  def main(args: Array[String]): Unit = {
    // flink环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = env.addSource(new SimulatedAdSource())
    val adEventStream = resource.assignAscendingTimestamps(_.timestamp)

    val filterBlackStream = adEventStream.keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUser(10))

    val adCountStream = filterBlackStream.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())

    adCountStream.print("count")
    filterBlackStream.getSideOutput(blackListOutputTag).print("blackList")
    env.execute("ad black statistic")


  }
  /*
过滤黑名单
 */
  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(String,Long),AdClickEvent,AdClickEvent]{
    // 定义状态，保存当前用户对当前广告的点击量
    lazy val countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("countState",classOf[Long]))
    // 保存是否发送过黑名单的状态
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isSent-state", classOf[Boolean]))
    // 保存定时器触发的时间戳
    lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resetTime-state", classOf[Long]))

    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(String, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      val curCount = countState.value()
      // 如果是第一次处理，则注册定时器，每天0点触发
      if (curCount == 0) {
        val timestamp = ( ctx.timerService().currentProcessingTime()/(1000*60*60*24) + 1) * (1000*60*60*24)
        resetTimer.update(timestamp)
        ctx.timerService().registerProcessingTimeTimer(timestamp)
      }
      if (curCount >= maxCount) {
        if (!isSentBlackList.value()) {
          isSentBlackList.update(true)
          ctx.output(blackListOutputTag,BlackListWarning(value.userId.toLong,value.adId,"Click over "+maxCount+" times today."))
        }
        return
      }
      countState.update(curCount+1)
      out.collect(value)
    }
    // 如果时间戳等与每天触发的时间点，那么就重置状态
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(String, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      if (timestamp == resetTimer.value()) {
        isSentBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }
    }
  }
}
// 自定义聚合函数
class AdCountAgg() extends AggregateFunction[AdClickEvent,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}
// 自定义窗口函数
class AdCountResult() extends WindowFunction[Long,CountByProvince,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(new Timestamp(window.getEnd).toString,key,input.iterator.next()))
  }
}


/*
生成随机数据，
 */
class SimulatedAdSource() extends RichSourceFunction[AdClickEvent]{
  var running = true
  val random = new Random()
  // 定义广告id种类,以7种广告为例吧
  val adTypes = Seq("1000", "10001", "10002", "10003", "10004", "10005", "10006", "10007")
  // 定义省份，城市
  val provinceAndCity = Map(
  "beijing" -> Seq("beijing"),
  "anhui" -> Seq("hefei", "suzhou", "luan", "fuyang"),
  "shanghai" -> Seq("shanghai"),
  "zhejiang" -> Seq("hangzhou", "wenzhou", "ningbo"))
  // map的key转为数组
  val array: Array[String] = provinceAndCity.keys.toArray

  override def run(ctx: SourceFunction.SourceContext[AdClickEvent]): Unit = {
    val maxElements = Long.MaxValue
    var count = 0L
    while( running && count < maxElements){
      val userId = random.nextInt(100).toString // 用户id
      val adId = adTypes(random.nextInt(adTypes.size))  // 广告id
      val provinceTypes = array(random.nextInt(array.size))  // 获得省份
      val provinceList = provinceAndCity(provinceTypes)
      val cityTypes = provinceList(random.nextInt(provinceList.size)) // 获得城市
      val timestamp = System.currentTimeMillis()

      ctx.collect(AdClickEvent(userId,adId.toLong,provinceTypes,cityTypes,timestamp))
      count += 1
      TimeUnit.MILLISECONDS.sleep(10L) // 每次循环休息1秒
    }
  }
  override def cancel(): Unit = false
}
