package cn.github.learningSpark

import org.apache.spark.{HashPartitioner, SparkContext}

object ComplexJob {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "ComplexJob")
    val data1 = Array[(Int, Char)](
      (1, 'a'), (2, 'b'),
      (3, 'c'), (4, 'd'),
      (5, 'e'), (6, 'f'),
      (7, 'g'), (8, 'h')
    )
    val rangePairs1 = sc.parallelize(data1, 3)
    val hashPairs1 = rangePairs1.partitionBy(new HashPartitioner(3))

    val data2 = Array[(Int, String)](
      (1, "A"), (2, "B"),
      (3, "C"), (4, "D")
    )
    val pairs2 = sc.parallelize(data2, 2)
    val rangePairs2 = pairs2.map {
      x => (x._1, x._2.charAt(0))
    }

    val data3 = Array[(Int,Char)](
      (1,'X'), (2,'Y')
    )
    val rangePairs3 = sc.parallelize(data3,2)

    val rangePairs = rangePairs2.union(rangePairs3)
    val result = hashPairs1.join(rangePairs)

    println(result.toDebugString)
    result.foreach(println)

    Thread.sleep(100000)

  }

}
