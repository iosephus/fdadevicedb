import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf

import scopt.OptionParser

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
    }
    
    val parser = new OptionParser[Config]("App") {

	head("App", "")
        opt[String]('d', "database") required()  action { (x, c) =>
        c.copy(database = x) } text("JDBC database URL (required)")
    }

    case class Config(database: String = "")
}

