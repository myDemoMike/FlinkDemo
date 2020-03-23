package com.my.base.transform

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Author: Yuan Liu
  * @Description: 对于ConnectedStreams 类型的数据集不能直接进行类似Print()的操作，
  *              需要再转换成DataStream 类型数据集，
  *              在Flink 中ConnectedStreams 提供的map()方法和flatMap()
  * @Date: Created in 13:49 2020/3/18
  *
  *        Good Good Study Day Day Up
  */
object ConnectTransformation {
  def main(args: Array[String]): Unit = {
    //初始化Flink的Streaming（流计算）上下文执行环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.streaming.api.scala._
    //创建不同数据类型的数据集
    val dataStream1: DataStream[(String, Int)] = streamEnv.fromElements(("a", 3),
      ("d", 4), ("c", 2), ("c", 5), ("a", 5))
    val dataStream2: DataStream[Int] = streamEnv.fromElements(1, 2, 4, 5, 6)
    //连接两个DataStream数据集
    val connectedStream: ConnectedStreams[(String, Int), Int] =
      dataStream1.connect(dataStream2)
    //coMap函数处理
    val result: DataStream[(Any, Int)] = connectedStream.map(
      //第一个处理函数
      t1 => {
        (t1._1, t1._2)
      },
      //第二个处理函数
      t2 => {
        (t2, 0)
      }

    )
    result.print()
    streamEnv.execute()
  }
}
