package cn.hust.gao.RDD

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavoriteTeacher {

  /*
  * 需求:
  * 官网有学科，每个学科有很多老师授课，选出每个学科中被访问量最多的那个老师
  * TopN问题
  * */
  def main(args: Array[String]): Unit = {
    //记录程序起始时间
    val starttime = System.nanoTime

    val conf = new SparkConf()
      .setAppName("FavoriteTeacher")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //读取数据
    val input = "file:///Users/gao/github/learningSpark/data/FavoriteTeacher"
    val lines = sc.textFile(input)

    //将数据转换为(subject,teacher)
    //    val subjectTeacherTuple = lines.map(line => {
    //      //http://www.6high.com/bigdata/laozhao
    //      val r: Regex = "http://.+/(.+)/(.+)".r
    //      val r(subjct, teacher) = line
    //      (subjct, teacher)
    //    })

    //将数据转换为(subject,teacher)
    val subjectTeacherTuple = lines.map(line => {
      val url = new URL(line)
      val path = url.getPath.split("[/]")
      val subject = path(1)
      val teacher = path(2)
      (subject, teacher)
    })

    //将数据转换为[(subject, teacher), 1],再进行聚合
    val teacherAndOne = subjectTeacherTuple.map((_, 1))
    val reduced = teacherAndOne.reduceByKey(_ + _)

    //格式转换 [(subject, teacher), 1] => [subject, (teacher,1)]
    //分组取第一
    //val result = reduced.map(x => (x._1._1, (x._1._2, x._2)))
    //.groupByKey()
    //.mapValues(_.toList.sortBy(_._2).reverse.take(1)(0))

    val result: RDD[(String, List[((String, String), Int)])] = reduced.groupBy(_._1._1)
      .mapValues(_.toList.sortBy(_._2).reverse.take(2))
    //分组取第一

    result.collect().foreach(println)

    // 打印程序结束时间
    val endtime = System.nanoTime
    val delta = endtime - starttime
    println("作业执行时间: "+delta / 1000000d)

  }

}
