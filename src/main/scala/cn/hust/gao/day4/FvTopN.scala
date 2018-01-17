package cn.hust.gao.day4

import org.apache.spark.{SparkConf, SparkContext}

object FvTopN {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("FvTopN")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val lines = sc.parallelize(List(
      "2,laoduan,13,100",
      "3,laoyang,20,100",
      "4,laoliu,25,80",
      "1,laoxu,24,8888"
    ))

    val userRDD = lines.map(line => {
      val fields = line.split("[,]")
      // [ ] 是转义符
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toInt
      (id, name, age, fv)
    })
    val sorted = userRDD.sortBy(t => User(t._1, t._2, t._3, t._4))
    println(sorted.collect().toBuffer)

  }

  case class User(id: Long, name: String, age: Int, fv: Int) extends Comparable[User] {
    override def compareTo(that: User): Int = {
      if (this.fv == that.fv) {
        this.age - that.age
      } else {
        that.fv - this.fv
      }
    }
  }

}
