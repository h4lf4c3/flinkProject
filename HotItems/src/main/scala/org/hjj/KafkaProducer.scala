package org.hjj

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer {
  def main(args: Array[String]): Unit = {
//    writeToKafka("HotItems")
    writeToKafka("user_behavior")
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
    for (line <- bufferedSource.getLines()) {
      println(line)
      val record = new ProducerRecord[String, String](topic, line) // 一行一行send到kafka
      producer.send(record)
      Thread.sleep(500)
    }
    producer.close()
  }
}