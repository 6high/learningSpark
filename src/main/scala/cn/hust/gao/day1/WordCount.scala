package cn.hust.gao.day1

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
//    val input = args(0)
//    val output = args(1)

    //读取 hdfs 数据
    val input = "/data/train.csv"

    val sparkConf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val result = sc.textFile(input)
      .flatMap((_.split(" ")))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2,false)

//    result.saveAsTextFile(output)
    result.foreach(println)
    sc.stop()
  }
}
