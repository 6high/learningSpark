package cn.hust.gao.wordCount

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object WoedCount_DataSet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("WoedCount2")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val data: Dataset[String] = spark.read.textFile(args(0))
    val result = data.flatMap(_.split(" "))
      .filter(_ != "")
      .map((_, 1))
      .toDF()
      .groupBy($"_1")
      .agg(count("*") as "num")
      .orderBy($"num" desc)

    result.show(false)

    spark.stop()
  }

}
