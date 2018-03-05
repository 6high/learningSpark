package cn.hust.gao.wordCunt;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Java7WordCount {

  public static void main(String[] args) {

    //创建java的spark程序入口
    SparkConf sparkConf = new SparkConf();
    sparkConf.setAppName("Java7WordCount").setMaster("local[*]");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    String input = "file:///Users/gao/Downloads/helloSpark";
    JavaRDD<String> lines = jsc.textFile(input,3);

    //切分压平
    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String line) throws Exception {
        // 对每一行的进行切分
        String[] wordArray = line.split(" ");
        // 将数据转换为迭代器再返回
        return Arrays.asList(wordArray).iterator();
      }
    });

    //将单词和1组合到一起 <word,1>
    JavaPairRDD<String, Integer> wordAndOne = words
        .mapToPair(new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String word) throws Exception {
            return new Tuple2<>(word, 1);
          }
        });

    //聚合
    JavaPairRDD<String, Integer> reduced = wordAndOne
        .reduceByKey(new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer v1, Integer v2) throws Exception {
            return v1 + v2;
          }
        });

    //排序,但是java的RDD只支持sortByKey,但是key是单词不是次数
    //解决方法:调换单词和次数的位置 <word,1> -> <1,word>
    JavaPairRDD<Integer, String> swaped = reduced
        .mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
          @Override
          public Tuple2<Integer, String> call(Tuple2<String, Integer> tp)
              throws Exception {
            //单词和次数换位置
            //return new Tuple2<>(tp._2,tp._1);
            return tp.swap();
          }
        });
    //排序
    JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
    //再次调换顺序
    JavaPairRDD<String, Integer> result = sorted
        .mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(Tuple2<Integer, String> tp)
              throws Exception {
            return tp.swap();
          }
        });

    result.saveAsTextFile("file:///Users/gao/Downloads/Java_wordCount_result");

  }

}
