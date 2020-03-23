package test

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * @Author: Yuan Liu
  * @Description:
  * @Date: Created in 13:43 2020/3/18
  *
  *        Good Good Study Day Day Up
  */
object TestUnion {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //创建不同的数据集
    val dataStream1: DataSet[(String, Int)] = env.fromElements(("a", 3), ("d", 4), ("c",
      2), ("e", 5), ("a", 5))
    val dataStream2: DataSet[(String, Int)] = env.fromElements(("d", 1), ("s", 2), ("a",
      4), ("e", 5), ("a", 6))
    val dataStream3: DataSet[(String, Int)] = env.fromElements(("a", 2), ("d", 1), ("s",
      2), ("c", 3), ("b", 1))
    //合并两个DataStream数据集
    val allUnionStream = dataStream1.union(dataStream2)
    allUnionStream.print()
  }

}
