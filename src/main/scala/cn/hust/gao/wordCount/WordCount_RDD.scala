package cn.hust.gao.wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount_RDD {
  def main(args: Array[String]): Unit = {

    //读取 hdfs 数据
    //val input = "/data/train.csv"

    //读取本地文件
    val input = "file:///Users/gao/Downloads/helloSpark"

    val sparkConf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // 读取数据,将数据分为三个区域,并缓存
    val lines = sc.textFile(input, 3)
    lines.cache()

    // 对每个分区的每一行数据进行切分,并压平
    val words: RDD[String] = lines.flatMap(line => line.split(" "))

    // 将单词映射成K-V形式
    val wordAndOne: RDD[(String, Int)] = words.map(word => (word, 1))

    // 对相同key的value进行合并
    val wordCount: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)

    // 对单词数量进行排序val result = value
    var result: RDD[(String, Int)] = wordCount.sortBy(_._2, false)

    // 收集数据打印
    result.collect().foreach(println)

    // 让程序沉睡10000000毫秒,我们可以去 http://localhost:4040
    // 查看Spark UI界面
    Thread.sleep(10000000)
    sc.stop()
  }
}
