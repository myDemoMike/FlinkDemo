package com.my.base

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._

object FlinkBatchDemo {

  private val ZOOKEEPER_HOST = "10.31.1.122:2181"
  private val KAFKA_BROKER = "10.31.1.122:9092"
  private val TRANSACTION_GROUP = "test"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.enableCheckpointing(1000)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//
//    // configure Kafka consumer
//    val kafkaProps = new Properties()
//    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
//    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
//    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
//
//    //topicd的名字是test，schema默认使用SimpleStringSchema()即可
//    val myConsumer = new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), kafkaProps)
//    val stream = env
//      .addSource(myConsumer)
    val stream = env.socketTextStream("10.31.1.122",9999)
    val  output = stream.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }//nonEmpty非空的
      .map { (_, 1) }
      .keyBy(0)//通过Tuple的第一个元素进行分组
      .timeWindow(Time.seconds(5))//Windows 根据某些特性将每个key的数据进行分组 (例如:在5秒内到达的数据).
      .sum(1)
    output.print()
    env.execute()
  }
}
