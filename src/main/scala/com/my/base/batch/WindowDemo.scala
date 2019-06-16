package com.my.base.batch

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object WindowDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
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

    // 每隔5秒中的翻滚操作
    //    val  output = stream.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
    //      .map { (_, 1) }
    //      .keyBy(0)
    //      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    //      .sum(1)


    // 每隔5秒中处理前10S的出局
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
