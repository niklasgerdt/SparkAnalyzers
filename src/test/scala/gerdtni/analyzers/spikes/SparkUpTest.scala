package gerdtni.analyzers.spikes

import gerdtni.analyzers.BaseTest
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest._

class SparkUpTest extends BaseTest {
  val conf = new SparkConf().setAppName("spark-up").setMaster("local")
  var sc: SparkContext = new SparkContext(conf)

  before {
    sc.stop()
    sc = new SparkContext(conf)
  }

  after {
    sc.stop()
  }

  "spike.txt" should "contain 6 rows" in {
    assert(SparkUp.analyze(sc) == 6)
  }
}