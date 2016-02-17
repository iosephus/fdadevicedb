import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf

import scopt.OptionParser
import scala.math.random
import fdadevices.nlp._

object SimpleApp {
    def main(args: Array[String]) {
	val config = parser.parse(args, Config())

	if (!config.isDefined)
	    System.exit(1)

        val conf = new SparkConf().setAppName("FDADevices")
        val sc = new SparkContext(conf)
	val sqlContext = new SQLContext(sc)
	val connInfo = Map("url" -> config.get.database, "dbtable" -> "pmn_applicant")
	val df = sqlContext.load("jdbc", connInfo)

	println(df.show())

	val slices = 6
	val n = 10000000 * slices
	val count = sc.parallelize(1 until n, slices).map { i =>
	val x = random * 2 - 1
	val y = random * 2 - 1
	if (x*x + y*y < 1) 1 else 0
	}.reduce(_ + _)
	println("Pi is roughly " + 4.0 * count / n)
	sc.stop()
    }
    
    val parser = new OptionParser[Config]("App") {

	head("App", "")
        opt[String]('d', "database") required()  action { (x, c) =>
        c.copy(database = x) } text("JDBC database URL (required)")
    }

    case class Config(database: String = "")
}

