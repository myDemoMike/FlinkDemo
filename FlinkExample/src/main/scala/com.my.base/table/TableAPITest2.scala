package com.my.base.table

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
  * @Author: Yuan Liu
  * @Description: flink table 自定义udf
  *
UDF 自定义的函数
用户可以在Table API 中自定义函数类，常见的抽象类和接口是：
 ScalarFunction
 TableFunction
 AggregateFunction
 TableAggregateFunction
  * @Date: Created in 17:10 2020/3/22
  *
  *        Good Good Study Day Day Up
  */
object TableAPITest2 {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    //初始化Table API的上下文环境
    val tableEvn = StreamTableEnvironment.create(streamEnv)
    //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._
    // val stream = streamEnv.readTextFile(getClass.getResource("/station.log").getPath)
    val stream: DataStream[String] =
      streamEnv.socketTextStream("10.31.1.125", 8888)
    val table: Table = tableEvn.fromDataStream(stream, 'words)
    var my_func = new MyFlatMapFunction()
    //自定义UDF
    val result: Table = table.flatMap(my_func('words)).as('word, 'count)
      .groupBy('word) //分组
      .select('word, 'count.sum as 'c) //聚合
    tableEvn.toRetractStream[Row](result)
      .filter(_._1 == true)
      .print()
    tableEvn.execute("table_api")
  }

  //自定义UDF
  class MyFlatMapFunction extends TableFunction[Row] {
    //定义类型
    override def getResultType: TypeInformation[Row] = {
      Types.ROW(Types.STRING, Types.INT)
    }

    //函数主体
    def eval(str: String): Unit = {
      str.trim.split(" ")
        .foreach({ word => {
          var row = new Row(2)
          row.setField(0, word)
          row.setField(1, 1)
          collect(row)
        }
        })
    }
  }

}

