package com.my.base


import com.my.base.source.StationLog
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
  * @Author: Yuan Liu
  * @Description: 统计每个手机的呼叫间隔时间，并输出
  * @Date: Created in 14:15 2020/3/22
  *
  *        Good Good Study Day Day Up
  */
object StateCallInterval {


  def main(args: Array[String]): Unit = {
    //初始化Flink的Streaming（流计算）上下文执行环境
    val streamEnv: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.streaming.api.scala._
    //读取文件数据
    val data = streamEnv.readTextFile(getClass.getResource("/station.log").getPath)
      .map(line => {
        val arr = line.split(",")
        new
            StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    data.keyBy(_.callInt) //按照呼叫手机号分组
      .flatMap(new CallIntervalFunction())
      .print()
//    data.keyBy(_.callInt) //按照呼叫手机号分组
//      .flatMapWithState[(String, Long), StationLog] {
//      case (in: StationLog, None) => (List.empty, Some(in)) //如果状态中没有，则存入
//      case (in: StationLog, pre: Some[StationLog]) => { //如果状态中有值则计算时间间隔
//        var interval = in.callTime - pre.get.callTime
//        (List((in.callInt, interval)), Some(in))enableCheckpointing
//      }
//    }.print()

//    data.keyBy(_.callInt) //按照呼叫手机号分组
//      .mapWithState[(String,Long),StationLog]{
//      case (in:StationLog,None)=>((in.callInt,0),Some(in))
//      case (in:StationLog,pre:Some[StationLog])=>{
//        var interval =in.callTime - pre.get.callTime
//        ((in.callInt,interval),Some(in))
//      }
//    }
    streamEnv.execute()
  }

  class CallIntervalFunction() extends
    RichFlatMapFunction[StationLog, (String, Long)] {
    //定义一个保存前一条呼叫的数据的状态对象
    private var preData: ValueState[StationLog] = _

    override def open(parameters: Configuration): Unit = {
      val stateDescriptor = new
          ValueStateDescriptor[StationLog]("pre", classOf[StationLog])
      preData = getRuntimeContext.getState(stateDescriptor)
    }

    override def flatMap(in: StationLog, collector: Collector[(String, Long)]): Unit = {
      val pre: StationLog = preData.value()
      if (pre == null) { //如果状态中没有，则存入
        preData.update(in)
      } else { //如果状态中有值则计算时间间隔
        val interval = in.callTime - pre.callTime
        collector.collect((in.callInt, interval))
      }
    }
  }


}
