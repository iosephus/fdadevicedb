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

	val result = df.map(row => (row.getAs[Int]("applicantid"), getApplicantNGrams(row))).collect().toMap

	for (i <- 1 to 10) {
	    println(result(i))
	}
    }
    
    def getApplicantNameTokens(row: org.apache.spark.sql.Row): Vector[String] = {
	val commonSuffixes = Vector("corp", "inc", "llc", "co", "ltd", "gmbh")
	val text = row.getAs[String]("applicant").toLowerCase
	val tokens = NGramsLibSimple.tokens(text)
	if (commonSuffixes contains tokens.last.replace(".", "")) tokens.slice(0, tokens.size - 1) else tokens
    }

    def getApplicantPostalCode(row: org.apache.spark.sql.Row): String = {
	if (row.getAs[String]("countrycode") ==  "US") {
	    val code = row.getAs[String]("zipcode")
	    if (code != null) code.take(5) else null
	} else {
	    row.getAs[String]("postalcode")
	}
    }

    def getApplicantAddressTokens(row: org.apache.spark.sql.Row): Vector[String] = {
	val addressComponents = for (column <- Vector("street1", "street2", "city")) yield row.getAs[String](column)

	val addressComponentsAll = addressComponents :+ getApplicantPostalCode(row)
	addressComponentsAll.filter(_ != null).map(s => s.toLowerCase.replace("[^a-z0-9]", "")).flatMap(NGramsLibSimple.tokens)
    }

    case class ApplicantNGrams(name: Set[Vector[String]], address: Set[Vector[String]], state: String, country: String)

    def getApplicantNGrams(row: org.apache.spark.sql.Row): ApplicantNGrams = {
	val nGramsOrders = Set(1, 2)
	val nameNGrams = NGramsLibSimple.uniqueNGrams(getApplicantNameTokens(row), nGramsOrders)
	val addressNGrams = NGramsLibSimple.uniqueNGrams(getApplicantAddressTokens(row), nGramsOrders)
	val state = row.getAs[String]("state")
	val country = row.getAs[String]("countrycode")
	ApplicantNGrams(nameNGrams, addressNGrams, state, country)
    }

    def ApplicantTokenMap(df: org.apache.spark.sql.DataFrame): Map[Int, ApplicantNGrams] = {

	df.map(row => (row.getAs[Int]("applicantid"), getApplicantNGrams(row))).collect().toMap
    }

    val parser = new OptionParser[Config]("App") {

	head("App", "")
        opt[String]('d', "database") required()  action { (x, c) =>
        c.copy(database = x) } text("JDBC database URL (required)")
    }

    case class Config(database: String = "")
}

