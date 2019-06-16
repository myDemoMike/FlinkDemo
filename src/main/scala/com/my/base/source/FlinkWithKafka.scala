package com.my.base.source

import java.util.Properties
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object FlinkWithKafka {

    private val ZOOKEEPER_HOST = "10.31.1.122:2181"
    private val KAFKA_BROKER = "10.31.1.122:9092"
    private val TRANSACTION_GROUP = "test"

    def main(args : Array[String]){
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      env.enableCheckpointing(1000)
      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

      // configure Kafka consumer
      val kafkaProps = new Properties()
      kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
      kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
      kafkaProps.setProperty("group.id", TRANSACTION_GROUP)

      //topicd的名字是test，schema默认使用SimpleStringSchema()即可
      val myConsumer = new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), kafkaProps)
      val stream = env
        .addSource(myConsumer)
      stream.print()
      env.execute()
    }
}
