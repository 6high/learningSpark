package cn.hust.gao.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavoriteUser2 {
  /**
    * 若已经明确要统计栏目，也可以采用如下简化的方式实现
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("FavoriteUser2").setMaster("local[*]"))
    sc.setLogLevel("WARN")
    val input = "file:///Users/gao/github/learningSpark/data/FavoriteUser"
    val lines = sc.textFile(input)
    val columnUserAndCounts: RDD[((String, String), Int)] = lines.map(line => {
      //医疗 user7 user7051
      val split: Array[String] = line.split("\\s+")
      val column: String = split(0)
      val user: String = split(1)
      ((column, user), 1)
    })
      .reduceByKey(_ + _)

    val columns = Array("教育", "法律", "医疗", "艺术", "音乐")
    for (column <- columns) {
      columnUserAndCounts.filter(t => column.equals(t._1._1))
        .top(1)(Ordering[Int].on(_._2))
        .map { case ((column, user), count) => s"$column $user $count" }
        .foreach(println)
    }
    sc.stop()
  }
}
