package com.iflytek.scala.outputformat

import org.apache.flink.api.common.io.{OutputFormat}
import org.apache.flink.configuration.Configuration
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import java.util.Properties;

class KafkaOutputFormat extends OutputFormat[String]{
  var servers = "192.168.56.101:9092"
  var topic = "Test_Topic_1"
  var acks = "all"
  var lingerMS = "1"
  var retries = "0"
  var batchSize = "10"
  var bufferMemory = "10240"

  var producer:Producer[String, String] = null

  override def configure(configuration: Configuration): Unit = {

  }

  override def open(i: Int, i1: Int): Unit = {
    val props = new Properties();
    props.setProperty("bootstrap.servers", servers);
    if (acks != null) {
      props.setProperty("acks", acks);
    }
    if (retries != null) {
      props.setProperty("retries", retries);
    }
    if (batchSize != null) {
      props.setProperty("batch.size", batchSize);
    }
    if (lingerMS != null) {
      props.setProperty("linger.ms", lingerMS);
    }
    if (bufferMemory != null) {
      props.setProperty("buffer.memory", bufferMemory);
    }
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producer = new KafkaProducer[String,String](props)

  }

  override def writeRecord(record: String): Unit = {
    println("record==",record)
    producer.send(new ProducerRecord[String,String](
      topic,
      String.valueOf(System.currentTimeMillis()),
      record));
  }

  override def close(): Unit = {
    producer.close()
  }

}