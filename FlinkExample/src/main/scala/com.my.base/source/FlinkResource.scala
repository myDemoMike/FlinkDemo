package com.my.base.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * @Author: Yuan Liu
  * @Description:
  * @Date: Created in 14:57 2019/7/16
  *
  *        Good Good Study Day Day Up
  */

// 定义一个数据样例类，传感器id，采集时间戳，传感器温度
case class SensorReading(id: String, timestamp: Long, temperature: Double)

class MyRedisMapper extends RedisMapper[SensorReading] {
  override def getCommandDescription: RedisCommandDescription = {
    // 定义保存到redis时的命令, hset sensor_temp sensor_id temperature
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
  }

  override def getValueFromData(t: SensorReading): String = t.temperature.toString

  override def getKeyFromData(t: SensorReading): String = t.id
}

object FlinkResource {

  private val ZOOKEEPER_HOST = "10.31.1.122:2181"
  private val KAFKA_BROKER = "10.31.1.122:9092"
  private val TRANSACTION_GROUP = "test"

  def main(args: Array[String]): Unit = {
    // 1. 获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    env.getConfig.setAutoWatermarkInterval(5000)
    // Checkpoint 开启和时间间隔指定：
    env.enableCheckpointing(1000)
    // Checkpoint 超时时间，默认为10分钟
    env.getCheckpointConfig.setCheckpointTimeout(50000)
    // 检查点之间最小时间间隔
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    // Checkpoint 超时时间：
    env.getCheckpointConfig.setCheckpointTimeout(50000)
    // 检查点之间最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(600)
    // 最大并行执行的检查点数量
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 是否删除Checkpoint 中保存的数据：
    //删除
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
    //保留
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置可以容忍的检查的失败数，超过这个数量则系统自动关闭和停止任务。
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(1)


    // 2. Source
    // Source1: 从集合读取
    val stream1 = env
      .fromCollection(List(
        SensorReading("sensor_1", 1547718199, 35.80018327300259),
        SensorReading("sensor_6", 1547718201, 15.402984393403084),
        SensorReading("sensor_7", 1547718202, 6.720945201171228),
        SensorReading("sensor_10", 1547718205, 38.101067604893444)
      ))
    stream1.print("stream1:").setParallelism(1)
    // Source2: 从文件读取
    val stream2 = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    stream2.print("stream2:").setParallelism(1)

    // Source3: kafka作为数据源
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("group.id", "consumer-group")
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
    kafkaProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProps.setProperty("auto.offset.reset", "latest")

    //topicd的名字是test，schema默认使用SimpleStringSchema()即可
    val myConsumer = new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), kafkaProps)
    val stream3 = env
      .addSource(myConsumer)
    stream3.print("stream3:").setParallelism(1)


    // 3. Transformation
    val dataStream = stream3.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      //      // 升序数据分配时间戳
      //      .assignAscendingTimestamps(_.timestamp * 1000)
      // 乱序数据分配时间戳和水印
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1000)) {
      override def extractTimestamp(element: SensorReading): Long = {
        element.timestamp * 1000
      }
    })
    //      .keyBy("id")
    //      .reduce( (x, y) => SensorReading( x.id, x.timestamp.min(y.timestamp) + 5, x.temperature + y.temperature ) )

    // 根据温度高低拆分流
    val splitStream = dataStream.split(data => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })

    val high = splitStream.select("high")
    val low = splitStream.select("low")
    val all = splitStream.select("high", "low")

    val warning = high.map(data => (data.id, data.temperature))
    val connected = warning.connect(low)

    val coMapStream = connected.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )

    val union = high.union(low, all, high).map(data => data.toString)

    // 统计每个传感器每3个数据里的最小温度
    val minTempPerWindow = dataStream.map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15), Time.seconds(5))
      //      .countWindow(3, 1)
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))

    minTempPerWindow.print().setParallelism(1)

    // 4. Sink
    //    high.print("high").setParallelism(1)
    //    low.print("low").setParallelism(1)
    //    all.print("all").setParallelism(1)
    //    coMapStream.print("coMap").setParallelism(1)

    // kafka sink
    //    union.print("union").setParallelism(1)
    //    union.addSink( new FlinkKafkaProducer011[String]( "localhost:9092", "sinkTest", new SimpleStringSchema() ) )

    // redis sink
    //    val conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build()
    //    dataStream.addSink( new RedisSink[SensorReading]( conf, new MyRedisMapper ) )

    // es sink
    //    val httpHosts = new util.ArrayList[HttpHost]()
    //    httpHosts.add( new HttpHost("localhost", 9200) )
    //    // 创建esSink的Builder
    //    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading]( httpHosts, new ElasticsearchSinkFunction[SensorReading] {
    //      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    //        println("saving data: " + t)
    //        val json = new util.HashMap[String, String]()
    //        json.put("sensor_id", t.id)
    //        json.put("timestamp", t.timestamp.toString)
    //        json.put("temperature", t.temperature.toString)
    //
    //        // 创建index request
    //        val indexReq = Requests.indexRequest().index("sensor").`type`("readingData").source(json)
    //        requestIndexer.add(indexReq)
    //        println("save successfully")
    //      }
    //    } )

    //    dataStream.addSink( esSinkBuilder.build() )
    env.execute("API test")
  }

}
