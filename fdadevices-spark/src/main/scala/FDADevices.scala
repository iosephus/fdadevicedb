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
	val connInfo = Map("url" -> config.get.database, "dbtable" -> "applicant")
	val df = sqlContext.load("jdbc", connInfo)

	
    }
    
    def getApplicantNameTokens(row) = {
	val commonSuffixes = Vector("corp", "inc", "llc", "co", "ltd", "gmbh")
	val text = row.applicant.toLowerCase()
	val tokens = NGramsLibSimple.tokens(text)
	if (commonSuffixes contains tokens.last.replace(".", "")) v.slice(0, v.size - 1) else v
    }

    def getApplicantAddressTokens(row) {
	row.select("street1", "street2", "city").filter(_ != null).map(toLowerCase).flatMap(NGramsLibSimple.tokens)
    }


    val parser = new OptionParser[Config]("App") {

	head("App", "")
        opt[String]('d', "database") required()  action { (x, c) =>
        c.copy(database = x) } text("JDBC database URL (required)")
    }

    case class Config(database: String = "")
}

