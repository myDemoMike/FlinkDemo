package com.my.base

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}

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
    val stream = env.socketTextStream("192.168.72.140", 9999)
    val output = stream.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    } //nonEmpty非空的
      .map {
      (_, 1)
    }
      .keyBy(0) //通过Tuple的第一个元素进行分组
      //   每隔2秒中计算最近5秒的数据   等价于  SlidingEventTimeWindows
      .timeWindow(Time.seconds(5), Time.seconds(2))
      .sum(1)
    output.print()




    env.execute()


  }
}
