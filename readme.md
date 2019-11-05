Apache Flink是一个框架和分布式处理引擎，用于对无界和有界数据流进行状态计算。

低延迟、高吞吐、结果的准确性和良好的容错性。

OLTP 联机事务处理。负责基本业务的正常运转。
OLAP 联机分析处理。探索并挖掘数据价值。
OLAP 在线分析系统，简单说就是报表系统，销售报表，统计报表，等等


在spark的世界观中，一切都是由批次组成的，离线数据是一个大批次，而实时数据是由一个一个无限的小批次组成的。
而在flink的世界观中，一切都是由流组成的，离线数据是有界限的流，实时数据是一个没有界限的流，这就是所谓的有界流和无界流。


所有的Flink程序都是由三部分组成的：  Source 、Transformation和Sink。


Environment
// getExecutionEnvironment会根据查询运行的方式决定返回什么样的运行环境，是最常用的一种创建执行环境的方式。
val env = StreamExecutionEnvironment.getExecutionEnvironment

// 返回本地执行环境，需要在调用时指定默认的并行度
val env = StreamExecutionEnvironment.createLocalEnvironment(1)

返回集群执行环境，将Jar提交到远程服务器。需要在调用时指定JobManager的IP和端口号，并指定要在集群中运行的Jar包。
val env = ExecutionEnvironment.createRemoteEnvironment("jobmanage-hostname", 6123,"YOURPATH//wordcount.jar")


Window
    Window可以分成两类：
        CountWindow：按照指定的数据条数生成一个Window，与时间无关。
        TimeWindow：按照时间生成Window。
    对于TimeWindow，可以根据窗口实现原理的不同分成三类：滚动窗口（Tumbling Window）、滑动窗口（Sliding Window）和会话窗口（Session Window）。


WindowAPI   都由滚动窗口和滑动窗口。
    TimeWindow是将指定时间范围内的所有数据组成一个window，一次对一个window里面的所有数据进行计算。
    CountWindow根据窗口中相同key元素的数量来触发执行，执行时只计算元素数量达到窗口大小的key对应的结果。

事件窗口：
EventTimeWindow API
    TumblingEventTimeWindows  滚动窗口
    SlidingEventTimeWindows  滑动窗口
    EventTimeSessionWindows  会话窗口

在Flink的流式处理中，绝大部分的业务都会使用eventTime，一般只在eventTime无法使用时，才会被迫使用ProcessingTime或者IngestionTime。


Table API是流处理和批处理通用的关系型API，Table API可以基于流输入或者批输入来运行而不需要进行任何修改。


ODS——操作性数据
    是作为数据库到数据仓库的一种过渡，ODS的数据结构一般与数据来源保持一致，
    便于减少ETL的工作复杂性，而且ODS的数据周期一般比较短。ODS的数据最终流入DW。
DW——数据仓库
    是数据的归宿，这里保持这所有的从ODS到来的数据，并长期保存，而且这些数据不会被修改。
    DWD：data warehouse detail 细节数据层，有的也称为 ODS层，是业务层与数据仓库的隔离层
    DWB：data warehouse base 基础数据层，存储的是客观数据，一般用作中间层，可以认为是大量指标的数据层。
    DWS：data warehouse service 服务数据层，基于DWB上的基础数据，整合汇总成分析某一个主题域的服务数据，一般是宽表。
DM——数据集市
    为了特定的应用目的或应用范围，而从数据仓库中独立出来的一部分数据，也可称为部门数据或主题数据。面向应用。

DB-ETL-DW-OLAP-DM-BI

MBA 
ACP 敏捷开发
