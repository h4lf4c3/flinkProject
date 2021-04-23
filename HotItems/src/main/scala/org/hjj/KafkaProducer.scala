package org.hjj

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object KafkaProducer {
  def main(args: Array[String]): Unit = {
//    writeToKafka("HotItems")
    writeToKafka("Behavior")
  }

  def writeToKafka(topic: String): Unit = {
    // 配置kafak属性参数
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 实例化kafak生产者
    val producer = new KafkaProducer[String, String](properties)
    // 从本地读取文件
//    val bufferedSource = io.Source.fromFile("D:\\Code\\javaCode\\UserBehaviorBaseFlink\\HotItems\\src\\main\\resources\\UserBehavior.csv")
    val bufferedSource = io.Source.fromFile("D:\\Code\\javaCode\\data\\UserBehavior\\UserBehavior.csv")
    val json = new JSONObject()
    for (line <- bufferedSource.getLines()) {
//      val dataArray = line.split(",")
//      json.put("user_id",dataArray(0))
//      json.put("item_id",dataArray(1))
//      json.put("category_id",dataArray(2))
//      json.put("behavior",dataArray(3))
//      json.put("ts",new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").format(dataArray(4).toLong *1000))
//      json.put("app_time",(dataArray(4).toLong * 1000L).toString)
      println(line)
      val record = new ProducerRecord[String, String](topic, line) // 一行一行send到kafka
      producer.send(record)
      Thread.sleep(50)
    }
    producer.close()
  }
}