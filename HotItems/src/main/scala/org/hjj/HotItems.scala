package org.hjj

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 控制台输出版本，实时打印浏览量排名前五的商品
 */
// 定义用户行为样例类 类成员为用户id，物品id，分类id，行为，时间戳
case class UserBehaviour(userId: Long, itemId: Long, categoryId: Int,behaviour: String, timestamp: Long)

//输出结果样例类 物品id，窗口结束时间，数量
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)
case class ItemViewCount2(itemId: String, count: String, windowEnd: String)
object HotItems {
  def main(args: Array[String]): Unit = {

    // 配置kafka属性参数
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // 创建flink环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 显式定义Time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1) // 设置并行度

    // 读取文件,数据源为kafka
     val stream = env.readTextFile("D:\\Code\\javaCode\\UserBehaviorBaseFlink\\HotItems\\src\\main\\resources\\UserBehavior.csv")
//    val stream = env.addSource(new FlinkKafkaConsumer[String]("HotItems",new SimpleStringSchema(),properties))
    stream.map(line => {
      val arrayLine = line.split(",") // 按照逗号进行分割,然后返回对象
      UserBehaviour(arrayLine(0).toLong, arrayLine(1).toLong, arrayLine(2).toInt, arrayLine(3), arrayLine(4).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000) // 指定时间戳和watermark
      .filter(_.behaviour == "pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy("windowEnd")
      .process(new TopNHotItems(5))
      .print()

    // 执行
    env.execute("Hot Items Job")
  }

  // 自定义聚合函数
  class CountAgg() extends AggregateFunction[UserBehaviour,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehaviour, accumulator: Long): Long = accumulator+1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a+b
  }

  // 自定义window function
  class WindowResultFunction() extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow]{
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
      val count = input.iterator.next()
      out.collect(ItemViewCount(itemId, window.getEnd, count))
    }
  }
  // 自定义process function
  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String]{
    // 定义状态ListState
    private var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 命名状态变量的名字和类型
      val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState",classOf[ItemViewCount])
      itemState = getRuntimeContext.getListState(itemStateDesc)
    }
    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      itemState.add(i)
      // 注册定时器，触发时间为windowEnd+1, 触发时说明window已经收集完成所有数据
      context.timerService().registerEventTimeTimer(i.windowEnd +1 )

    }
    // 定时器触发操作，从state中取出所有数据，排序并进行topN
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 获取所有商品点击信息
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (item <- itemState.get){
        allItems += item
      }
      // 清除状态中的数据，释放
      itemState.clear()
      // 按照点击量进行排序，从大到小，选取topN
      val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
      // 将排序后的数据美化输出
      val result: StringBuilder = new StringBuilder
      result.append("------------------------\n")
      result.append("Time: ").append(new Timestamp(timestamp-1)).append("\n")
      // 可以直接遍历index取到序号
      for (i <- sortedItems.indices){
        val currentItem: ItemViewCount = sortedItems(i)
        // 输出格式为
        result.append("No").append(i + 1).append(":")
          .append(" 商品ID=").append(currentItem.itemId)
          .append(" 浏览量=").append(currentItem.count)
          .append("\n")
      }

      result.append("--------------------\n\n")
      Thread.sleep(1000)
      out.collect(result.toString())
    }
  }

}

