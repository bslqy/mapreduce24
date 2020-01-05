package cn.edu360.spark.scala;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class JavaLambdaWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaWordCount");

        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile(args[0]);

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split("")).iterator());

        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(w -> new Tuple2<>(w, 1));

        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey((m, n) -> m + n);

        JavaPairRDD<Integer, String> swapped = reduced.mapToPair(tp -> tp.swap());

        JavaPairRDD<Integer, String> sorted = swapped.sortByKey(false);

        JavaPairRDD<String, Integer> result = sorted.mapToPair(tp -> tp.swap());

        result.saveAsTextFile(args[1]);


    }
}
