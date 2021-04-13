package org.hjj

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerLog {
  def main(args: Array[String]): Unit = {
    writeToKafka("apacheLog")
  }
  def writeToKafka(topic: String): Unit ={
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 实例化kafaka生产者
    val producer = new KafkaProducer[String, String](properties)
    val bufferedSource = io.Source.fromFile("D:\\Code\\javaCode\\UserBehaviorBaseFlink\\NetWorkFlow\\src\\main\\resources\\apache.log")
    for(line <- bufferedSource.getLines()){
      println(line)
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }
}
