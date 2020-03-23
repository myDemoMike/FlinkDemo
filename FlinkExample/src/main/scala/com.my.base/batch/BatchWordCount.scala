package com.my.base.batch

import org.apache.flink.api.scala.ExecutionEnvironment


/**
  * @Author: Yuan Liu
  * @Description:
  * @Date: Created in 22:21 2020/3/14
  *
  *        Good Good Study Day Day Up
  */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    //初始化flink的环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.api.scala._
    //读取数据
    val dataURL = getClass.getResource("/wc.txt")
    //wc.txt文件在main目录下的resources中
    val data: DataSet[String] = env.readTextFile(dataURL.getPath)
    //计算
    val result: AggregateDataSet[(String, Int)] = data.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0) //其中0代表元组中的下标，“0”下标代表：单词
      .sum(1) //其中1代表元组中的下标，“1”下标代表：单词出现的次数
    //打印结果
    result.print()
  }
}
