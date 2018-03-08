package cn.hust.gao.RDD

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object FavoriteUser {
  /*
  * 需求:
  * 有一个微博网站有很多栏目，每一个栏目都有几千万用户，每个用户会有很多的粉丝
  * 要求取出各栏目粉丝量最多的用户
  *
  * 代码实现:
  * 对于选出最受欢迎的老师的需求，我们只需要预聚合后，进行groupBy,
  * 然后对每一个key里面的value用scala集合的方法排序即可。
  * 但对于该需求，预聚合后，每个分区仍然有几千万的用户需要排序统计
  * 如果进行groupBy，一个分区内的数据会全部转成内存中的数组，有内存溢出的危险
  * 我们可以利用repartitionAndSortWithinPartitions按照key进行排序，并根据栏目指定好分区
  * 这样排序后，只需要取出每个分区里的第一个元素，就是每个栏目粉丝量最多的用户
  * */
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("FavoriteUser").setMaster("local[*]"))
    sc.setLogLevel("WARN")
    val input = "file:///Users/gao/github/learningSpark/data/FavoriteUser"
    val lines = sc.textFile(input)
    val columnUserTuple = lines.map(line => {
      //医疗 user7 user7051
      val split = line.split("\\s+")
      (split(0), split(1))
    })

    val columns = columnUserTuple.map(_._1).distinct().collect()
    //申明一个Ordering[(String,Int)]类型的隐式值，用于修改默认的排序规则
    implicit val sortkey = Ordering[(String, Int)].on[(String, Int)](x => (x._1, -x._2))
    val values = columnUserTuple
      .map((_, 1))
      .reduceByKey(_ + _)
      .map(x => ((x._1._1, x._2), x._1._2))
      //按照指定的分区器进行shuffle分区，并对key进行排序，
      //由于上面申明Ordering[(String,Int)]类型的隐式值，原始的排序将会被修改
      .repartitionAndSortWithinPartitions(new Partitioner {
      private val rules: Map[String, Int] = columns.zipWithIndex.toMap

      override def numPartitions: Int = rules.size + 1

      override def getPartition(key: Any): Int = {
        val column = key.asInstanceOf[(String, Int)]._1
        rules.getOrElse(column, -1) + 1
      }
    })
      //由于每个分区都是粉丝数量最多的用户，所以只需取出
      //每个分区第一个元素及为该栏目粉丝量最多的用户
      .mapPartitions(it => {
      if (it.hasNext) Iterator.single(it.next())
      else it
    })
      .foreach(println)


  }
}
