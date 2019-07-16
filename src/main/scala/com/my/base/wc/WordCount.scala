package com.my.base.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
/**
  * Implements the "WordCount" program that computes a simple word occurrence
  * histogram over text files in a streaming fashion.
  *
  * The input is a plain text file with lines separated by newline characters.
  *
  * Usage:
  * {{{
  * WordCount --input <path> --output <path>
  * }}}
  *
  * If no parameters are provided, the program is run with default data from
  * {@link WordCountData}.
  *
  * This example shows how to:
  *
  *  - write a simple Flink Streaming program,
  *  - use tuple data types,
  *  - write and use transformation functions.
  *
  */
object WordCount {

  def main(args: Array[String]) {

    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // get input data
    val text =
    // read the text file from given input path
      if (params.has("input")) {
        env.readTextFile(params.get("input"))
      } else {
        val inputPath = "D:\\Projects\\BigData\\FlinkDemo\\src\\main\\resources\\hello.txt"
        env.readTextFile(inputPath)
        // env.fromElements(WordCountData.WORDS: _*)
      }
    // groupBy 与 keyBy
    val counts: DataStream[(String, Int)] = text
      // split up the lines in pairs (2-tuples) containing: (word,1)
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      // group by the tuple field "0" and sum up tuple field "1"
      .keyBy(0)
      .sum(1)
    // emit result
    if (params.has("output")) {
      counts.writeAsText(params.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }

    // execute program
    env.execute("Streaming WordCount")
  }
}
