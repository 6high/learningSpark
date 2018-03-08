package cn.hust.gao.test

import java.net.URL

object MyScalaTest {
  def main(args: Array[String]): Unit = {
    val line = "http://www.6high.com/javaEE/xiaoxu"
    val url = new URL(line)
    val path = url.getPath.split("[/]")
    val subject = path(1)
    val teacher = path(2)
    println(s"$subject    $teacher")
  }
}
