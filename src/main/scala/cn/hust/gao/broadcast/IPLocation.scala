package cn.hust.gao.broadcast


import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object IPLocation {

  // mysql的连接属性
  val url = "jdbc:mysql://127.0.0.1:3306/bigdata"
  val username = "root"
  val password = "123456"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("IPLocation")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val ipRulePath = "file:///Users/gao/github/learningSpark/data/ip.csv"
    val cdnLogPath = "file:///Users/gao/github/learningSpark/data/cdn.txt"


    //ipRulesRdd将作为广播变量,要尽量减小空间占用(减小空间占用,减小传输开销)
    val ipRulesRdd = sc.textFile(ipRulePath).map(line => {
      val fields = line.split(",")
      val start_num = fields(1).toLong
      val end_num = fields(2).toLong
      val province = fields(3)
      (start_num, end_num, province)
    })
    //收集全部映射规则
    val ipRulesArray = ipRulesRdd.collect()
    //将映射规则映射出去
    val ipRulesBroadcast: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipRulesArray)

    //加载要处理的cdn日志数据的ip
    val ipsRDD = sc.textFile(cdnLogPath).map(_.split(" ")(0))

    val result = ipsRDD.map(ip => {
      val ipNum = ip2Long(ip)
      val index = binarySearch(ipRulesBroadcast.value,ipNum)
      val info = ipRulesBroadcast.value(index)
      info
    }).map(t =>(t._3,1)).reduceByKey(_+_)

    //结果写入数据库
    result.foreachPartition(data2mysql(_))
    sc.stop()

  }


  /**
    * 将迭代器的数据存入mysql
    */
  val data2mysql = (iter: Iterator[(String, Int)]) => {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "INSERT INTO location_info (location, counts, cdn_date) VALUES (?, ?, ?)"

    conn = DriverManager.getConnection(url, username, password)
    try {
      iter.foreach(line => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, line._1)
        ps.setInt(2, line._2)
        ps.setDate(3, new Date(System.currentTimeMillis()))
        ps.executeUpdate()
      })
    } catch {
      case e: Exception => println("mysql Exception\n", e)
    } finally {
      if (ps != null) ps.close()
      else conn.close()
    }
  }

  /**
    * 将ip字符串转换为长整型
    *
    * @param ipStr
    * @return
    */
  def ip2Long(ipStr: String): Long = {
    val fragments = ipStr.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }


  /**
    * 二分法查找ip所在范围
    *
    * @param arr
    * @param ip
    * @return
    */
  def binarySearch(arr: Array[(Long, Long, String)], ipNum: Long): Int = {
    var low = 0
    var high = arr.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      val startNum = arr(middle)._1
      val endNum = arr(middle)._2
      if (startNum <= ipNum && ipNum <= endNum) {
        return middle
      }
      else if (startNum > ipNum) {
        high = middle - 1
      }
      else {
        low = middle + 1
      }
    }
    0
  }

}
