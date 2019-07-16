package com.my.base.window

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Flink任务的2种提交方式
  *
  * 1.提交到Flink集群 standalone模式
  *   ./flink run -c com.my.base.batch.WindowDemo  /home/liuyuan/FlinkDemo-jar-with-dependencies.jar --hostname 10.31.1.123
  * 2.提交到Hadoop集群
  *   1）./yarn-session.sh -n 2 -s 2 -jm 1024 -nm test -d
  *   2）./flink run -m yarn-cluster  -c com.my.base.batch.WindowDemo  /home/liuyuan/FlinkDemo-jar-with-dependencies.jar --hostname 10.31.1.123
  *
  */
object WindowDemo {

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val hostname =
      if (params.has("hostname")) {
        params.get("hostname")
      } else {
        println("请输入hostname")
        ""
      }
    // getExecutionEnvironment会根据查询运行的方式决定返回什么样的运行环境，是最常用的一种创建执行环境的方式。
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream(hostname, 9999)
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

    // 每隔5秒中的翻滚操作
    //    val  output = stream.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
    //      .map { (_, 1) }
    //      .keyBy(0)
    //      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    //      .sum(1)


    // 每隔5秒钟处理前10S的出局
    //    val output = stream.flatMap {
    //      _.toLowerCase.split("\\W+") filter {
    //        _.nonEmpty
    //      }
    //    }
    //      .map {
    //        (_, 1)
    //      }
    //      .keyBy(0)
    //      .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
    //      .sum(1)


    //    val output = stream.flatMap {
    //      _.toLowerCase.split("\\W+") filter {
    //        _.nonEmpty
    //      }
    //    } //nonEmpty非空的
    //      .map {
    //      (_, 1)
    //    }
    //      .keyBy(0) //通过Tuple的第一个元素进行分组
    //      // 每当窗口中填满100的元素时，就会对窗口进行计算， 翻滚窗口
    //      .countWindow(100)
    //      .sum(1)

    //    val output = stream.flatMap {
    //      _.toLowerCase.split("\\W+") filter {
    //        _.nonEmpty
    //      }
    //    } //nonEmpty非空的
    //      .map {
    //      (_, 1)
    //    }
    //      .keyBy(0) //通过Tuple的第一个元素进行分组
    //      // 计算每10个元素计算一次最近100个元素的总和
    //      .countWindow(100,10)
    //      .sum(1)


    //    val output = stream.flatMap {
    //      _.toLowerCase.split("\\W+") filter {
    //        _.nonEmpty
    //      }
    //    } //nonEmpty非空的
    //      .map {
    //      (_, 1)
    //    }
    //      .keyBy(0) //通过Tuple的第一个元素进行分组
    //    // 如果用户30秒没有活动则视为会话断开（假设raw data stream是单个用户的购买行为流）
    //      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)))
    //      .sum(1)
    output.print()
    env.execute()
  }
}
