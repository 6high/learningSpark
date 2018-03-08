package cn.hust.gao.broadcast

import scala.io.{BufferedSource, Source}

object IPLocationDemo {
  def main(args: Array[String]): Unit = {
    val ipData: Array[String] = readData("/Users/gao/github/learningSpark/data/ip.csv")
    println(ipData(binarySearch(ipData, ip2Long("222.20.38.116"))))

  }

  def ip2Long(ipStr: String): Long = {
    val fragments = ipStr.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def readData(path: String): Array[String] = {
    val source: BufferedSource = Source.fromFile(path)
    //对迭代器应用toArray或toBuffer方法，将这些行放到数组或数组缓冲中
    val lines = source.getLines.toArray
    source.close() //记得调用close
    lines
  }

  def binarySearch(arr: Array[String], ip: Long): Int = {
    var low = 0
    var high = arr.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      val startIp = arr(middle).split(",")(1).toLong
      val endIp = arr(middle).split(",")(2).toLong
      if (startIp <= ip && ip <= endIp){
        return middle
      }
      else if (startIp > ip){
        high = middle - 1
      }
      else {
        low = middle + 1
      }
    }
    -1
  }

}
