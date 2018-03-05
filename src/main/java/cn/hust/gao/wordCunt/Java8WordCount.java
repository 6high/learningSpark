package cn.hust.gao.wordCunt;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Java8WordCount {

  // java8 lambda 函数式编程
  public static void main(String[] args) {
    //启动spark
    SparkConf sparkConf = new SparkConf().setAppName("Java8WordCount").setMaster("local[*]");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    //读取数据
    String input = "file:///Users/gao/Downloads/helloSpark";
    JavaRDD<String> lines = jsc.textFile(input);

    //切分压平
    JavaRDD<String> words = lines
        .flatMap(line -> Arrays.asList(line.split(" ")).iterator());

    //组装单词和1
    JavaPairRDD<String, Integer> wordAndOne = words
        .mapToPair(word -> new Tuple2<>(word, 1));

    //聚合
    JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey((x, y) -> x + y);

    //排序,只有sortByKey这个函数,所以要调换KV位置
    //调换位置
    JavaPairRDD<Integer, String> swaped = reduced.mapToPair(tp -> tp.swap());
    //排序
    JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
    //再调换位置
    JavaPairRDD<String, Integer> result = sorted.mapToPair(tp -> tp.swap());

    result.saveAsTextFile("file:///Users/gao/Downloads/Java8WordCount");


  }
}
