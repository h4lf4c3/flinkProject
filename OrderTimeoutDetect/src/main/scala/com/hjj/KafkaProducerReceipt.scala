package com.hjj

import java.util.Properties

import org.apache.commons.lang3.RandomStringUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object KafkaProducerReceipt {
  def main(args: Array[String]): Unit = {
    writeToKafka("receipt")
  }
  def writeToKafka(topic: String): Unit ={
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 实例化kafaka生产者
    val producer = new KafkaProducer[String, String](properties)
    val random = new Random()

    // payChannel类型序列
    val payChannelTypes = Seq("Wechat", "Alipay","UnionPay")
    var count = 0L // 控制循环
    var line = "" // 定义初始每行数据
    while (count < Long.MaxValue){
      val txId = random.nextInt(20)+300
      val payChannel = payChannelTypes(random.nextInt(payChannelTypes.size))
      val timestamp = System.currentTimeMillis()
      line = txId + "," + payChannel + "," + timestamp
      println(line)
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
      count += 1
      Thread.sleep(1000)
    }
    producer.close()
  }
}
