package org.hjj

import java.util.Properties
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object BloomFilter {
  def main(args: Array[String]): Unit = {
    // 配置kafka属性参数
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 定义source,从kafka中读取
    val resource = env.addSource(new FlinkKafkaConsumer[String]("PageView", new SimpleStringSchema(), properties))
    val dataStream = resource.map(data => {
      val dataArray = data.split(",")
      UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .map(data => ("dummykey",data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())
      .process(new UniqueWithBloom())

    dataStream.print()
    env.execute("unique")
  }
}

// 自定义触发器
class MyTrigger() extends Trigger[(String,Long),TimeWindow]{
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    // 每来一条数据，就直接触发窗口操作，并清空所有窗口状态
    TriggerResult.FIRE_AND_PURGE
  }
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
  }
}

class UniqueWithBloom() extends ProcessWindowFunction[(String,Long),UniqueCount,String,TimeWindow]{
  lazy val jedis = new Jedis("localhost",6379)
  lazy val bloom = new Bloom(1<<29)
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UniqueCount]): Unit = {
    val storeKey = context.window.getEnd.toString
    var count = 0L
    if(jedis.hget("count",storeKey)!=null){
      count = jedis.hget("count",storeKey).toLong
    }
    val userI = elements.last._2.toString
    val offset = bloom.hash(userI, 61)
    val isExist = jedis.getbit(storeKey, offset)

    if(!isExist){
      jedis.setbit(storeKey,offset,true)
      jedis.hset("count",storeKey,(count+1).toString)
    }
    else{
      out.collect(UniqueCount(storeKey.toLong,count))
    }
  }
}

// Bloom过滤器类
class Bloom(size: Long) extends Serializable{
  // 定义位图总大小，默认为16M
  private val cap = if(size>0) size else 1<<27
  // 定义hash函数
  def hash(value: String, seed: Int): Long ={
    var result = 0L
    for(i <- 0 until value.length){
      result = result * seed + value.charAt(i)
    }
    result & (cap-1)
  }
}