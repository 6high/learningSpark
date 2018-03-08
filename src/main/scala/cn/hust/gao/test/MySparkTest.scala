package cn.hust.gao.test

import org.apache.spark.{Partition, SparkConf, SparkContext}

object MySparkTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("MySparkTest").setMaster("local[*]"))
    sc.setLogLevel("WARN")


    val data = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    val dataStr = sc.parallelize(List("A","B","C","D","E","F"),6)

    val partitions: Array[Partition] = data.partitions

    def partPrint(index: Int, iter: Iterator[(Int)]): Iterator[String] = {
      iter.toList.map(x => s"partID: $index, val: $x").iterator
    }
    def partPrint1(index: Int, iter: Iterator[(String)]): Iterator[String] = {
      iter.toList.map(x => s"partID: $index,\t val: $x").iterator
    }

    //data.mapPartitionsWithIndex(partPrint).collect.foreach(println)

    val result = dataStr.mapPartitionsWithIndex((index, iter) =>
      iter.toList.map(x => s"partID: $index,\t val: $x").iterator
    ).collect()
    result.foreach(println)
//    println(result.toBuffer)


    //Thread.sleep(100000000)

  }

}
