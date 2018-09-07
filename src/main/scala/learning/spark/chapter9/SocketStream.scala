package learning.spark.chapter9

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by zhaoliu on 18-9-6.
 * 1. Using
 *    nc -l localhost 7777
 *  to generate sample data
 * 2. Create an empty SparkConf because using spark-submit to submit job
 *    It is a mistake to write:
 *       val conf = new SparkConf().setMaster("local").setAppName("socket-stream")
 *    setMaster("local") leads to only create on executor!
 */
object SocketStream extends App {
  val conf = new SparkConf()
  val streamCtx = new StreamingContext(conf, Seconds(10))

  val lines = streamCtx.socketTextStream("localhost", 7777)
  val errors = lines.filter(_.startsWith("ERROR"))
  errors.print()

  streamCtx.start()
  streamCtx.awaitTermination()
}
