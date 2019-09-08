package com.my.base.wc

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * @Author: Yuan Liu
  * @Description:
  * @Date: Created in 21:21 2019/9/8
  *
  *        Good Good Study Day Day Up
  */
object DataSetWc {


  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val txtDataSet: DataSet[String] = env.readTextFile("E:\\workspace\\FlinkDemo\\FlinkExample\\src\\main\\resources\\hello.txt")


    val result: AggregateDataSet[(String, Int)] = txtDataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    result.print()

  }
}
