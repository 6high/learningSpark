package cn.hust.gao.RDD

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import spire.std.unit

import scala.collection.mutable

object FavoriteTeacher2 {

  /*
  * 需求:
  * 官网有学科，每个学科有很多老师授课，选出每个学科中被访问量最多的那个老师
  * TopN问题
  *
  * 自定义分区
  * 先求出学科,根据学科分区对数据进行分区
  * */
  def main(args: Array[String]): Unit = {
    //记录程序起始时间
    val starttime = System.nanoTime


    val conf = new SparkConf()
      .setAppName("FavoriteTeacher")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //sc.setLogLevel("WARN")

    //读取数据
    val input = "file:///Users/gao/github/learningSpark/data/FavoriteTeacher"
    val lines = sc.textFile(input)


    //将数据转换为[(subject, teacher), 1]
    val subjectTeacherOne = lines.map(line => {
      val url = new URL(line)
      val path = url.getPath.split("[/]")
      val subject = path(1)
      val teacher = path(2)
      ((subject, teacher), 1)
    })

    //对(subject, teacher)进行聚合,cache到内存
    val reduced: RDD[((String, String), Int)] = subjectTeacherOne.reduceByKey(_ + _).cache()

    //求出学科 subject
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()
    //格式转换,以学科subject为key
    val reversed: RDD[(String, (String, Int))] = reduced.map(x => (x._1._1, (x._1._2, x._2)))

    //按照自定义分区器的规则进行shuffle
    val subjectPartition = new SubjectPartitioner(subjects)
    val partitioned = reversed.partitionBy(subjectPartition)
    val result = partitioned.mapPartitions(_.toList.sortBy(_._2._2).reverse.take(2).iterator)

    result.collect().foreach(println)

//    val partitionPrint: RDD[String] = partitioned.mapPartitionsWithIndex {
//      (i, iter) => iter.map(x => s"partID: $i,\t val:$x").toList.iterator
//    }
//    partitionPrint.collect().foreach(println)

    // 打印程序结束时间
    val endtime = System.nanoTime
    val delta = endtime - starttime
    println("作业执行时间: "+delta / 1000000d)
  }

  /**
    * 继承 Partitioner 类,重写 getPartition 方法
    */
  class SubjectPartitioner(subjects: Array[String]) extends Partitioner {
    //定义分区规则,在hashmap中对名称和数字
    //***这里还可以优化,每次调用分区器聚会形成一个rules,浪费时间开销***
    //!!!错误!!! new这个分区的时候的时候调用一次rules
    val rules = new mutable.HashMap[String, Int]()
    var i = 1
    for (sub <- subjects) {
      rules += (sub -> i)
      i += 1
    }

    //有几个分区
    override def numPartitions: Int = subjects.length + 1

    //根据传过来的key决定该条数据分配到哪个分区
    override def getPartition(key: Any): Int = {
      val k = key.toString
      // 找得到的放在对应的分区,找不到的脏数据放在0分区
      rules.getOrElse(k, 0)
    }
  }

}
