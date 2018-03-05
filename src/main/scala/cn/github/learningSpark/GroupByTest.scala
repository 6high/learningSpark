package cn.github.learningSpark

import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}

object GroupByTest {

  def main(args: Array[String]): Unit = {

    // spark 1.6 及以前使用此方式启动spark
    val sparkConf = new SparkConf()
      .setAppName("GroupByTest")
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val numMappers = 100
    var numKVPairs = 10000
    var valSize = 1000
    var numReducers = 36

    val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random()
      var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
      }
      arr1
    }.cache

    pairs1.take(100).foreach(println)
    pairs1.count

    println(pairs1.groupByKey(numReducers).count)

    sc.stop()
  }

}
