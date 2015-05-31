package gerdtni.analyzers.spikes

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import grizzled.slf4j.Logging

object SparkUp extends App with Logging {
  info("spiking up with Spark")
  val conf = new SparkConf().setAppName("spark-up").setMaster("local")
  val sc = new SparkContext(conf)
  analyze(sc)

  def analyze(sc: SparkContext) = {
    val set = sc.textFile("files/spike.txt", 2)
    val count = set.count()
    println("rows: " + count)
    count
  }
}