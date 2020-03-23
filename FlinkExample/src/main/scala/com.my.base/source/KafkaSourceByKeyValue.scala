package com.my.base.source

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer


/**
  * @Author: Yuan Liu
  * @Description: 读取kafka中的KeyValue数据
  * @Date: Created in 12:43 2020/3/18
  *
  *        Good Good Study Day Day Up
  */
object KafkaSourceByKeyValue {
  def main(args: Array[String]): Unit = {
    //初始化Flink的Streaming（流计算）上下文执行环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.streaming.api.scala._
    val props = new Properties()
    props.setProperty("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103: 9092")
    props.setProperty("group.id", "fink02")
    props.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("auto.offset.reset", "latest")

    //设置kafka为数据源
    val stream = streamEnv.addSource(new
        FlinkKafkaConsumer011[(String, String)]("t_topic",
          new KafkaDeserializationSchema[(String, String)] {
            //流是否结束
            override def isEndOfStream(t: (String, String)) = false

            override def deserialize(consumerRecord: ConsumerRecord[Array[Byte],
              Array[Byte]]) = {
              if (consumerRecord != null) {
                var key = "null"
                var value = "null"
                if (consumerRecord.key() != null)
                  key = new String(consumerRecord.key(), "UTF-8")
                if (consumerRecord.value() != null)
                  value = new String(consumerRecord.value(), "UTF-8")
                (key, value)
              } else { //如果kafka中的数据为空返回一个固定的二元组
                ("null", "null")
              }
            }

            //设置返回类型为二元组
            override def getProducedType =
              createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[
                String])
          }
          , props).setStartFromEarliest())
    stream.print()
    streamEnv.execute()
  }
}
