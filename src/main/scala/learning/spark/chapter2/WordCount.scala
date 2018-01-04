package learning.spark.chapter2

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhaoliu on 18-1-4.
 */
object WordCount extends App{

  if (args.length < 2) {
    Console.println("Usage: WordCount <input-file> <output-file>")
    System.exit(-1)
  }

  val conf = new SparkConf().setMaster("local").setAppName("WordCount")
  val sc = new SparkContext(conf)

  val input = sc.textFile(args(0))

  val words = input.flatMap(line => line.split(" "))

  val counts = words.map(word => (word, 1)).reduceByKey{ case (x, y) => x + y }

  counts.saveAsTextFile(args(1))
}
