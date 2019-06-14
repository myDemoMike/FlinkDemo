package com.my.base

case class Point(x: Double, y: Double)

object Main {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    import org.apache.flink.api.scala.extensions._
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
    val result = ds.filterWith {
      case Point(x, _) => x > 1
    }.reduceWith {
      case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
    }.mapWith {
      case Point(x, y) => (x, y)
    }.flatMapWith {
      case (x, y) => Seq("x" -> x, "y" -> y)
    }.groupingBy {
      case (id, value) => id
    }
    result.maxBy(1).print()
  }
}
