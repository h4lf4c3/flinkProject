package org.hjj

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 数据日志样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)
// 输出结果样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)


object NetWorkFlow {
  def main(args: Array[String]): Unit = {

    // 创建flink环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从linux网络监听中读取数据
    val dataStream = env.socketTextStream("localhost",8888)
      .map(data =>{
        val dataArray = data.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss") // 定义时间格式
        val timestamp = simpleDateFormat.parse(dataArray(3).trim).getTime
        ApacheLogEvent(dataArray(0).trim,dataArray(1).trim,timestamp,dataArray(5).trim,dataArray(6).trim)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10),Time.seconds(5))
      .allowedLateness(Time.seconds(60))  // 允许迟到的时间为60秒


  }
}


// 自定义聚合函数
class CountAgg() extends AggregateFunction[ApacheLogEvent,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}

// 自定义窗口函数
class WindowResult() extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key,window.getEnd,input.iterator.next()))
  }
}
// 自定义keyed process函数
class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{

  lazy val urlState: MapState[String,Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String,Long]("url-state",classOf[String],classOf[Long]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    urlState.put(value.url,value.count)
    ctx.timerService().registerEventTimeTimer(value.windowEnd+1)
  }
}