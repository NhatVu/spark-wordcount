package wordcount;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) {
		String dataFile = args[0];
		String resultFolder = args[1];
//		String dataFile = "/home/minhnhat/VCCorp/test.txt";
//		String resultFolder = "/home/minhnhat/VCCorp/result";
		SparkConf conf = new SparkConf().setAppName("My App");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> textFile = sc.textFile(dataFile);
		JavaPairRDD<String, Integer> counts = textFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
		        .mapToPair(word -> new Tuple2<String, Integer>(word, 1)).reduceByKey((a, b) -> a + b);

		counts.saveAsTextFile(resultFolder);
		System.out.println(counts.collect());
		sc.close();
	}
}
