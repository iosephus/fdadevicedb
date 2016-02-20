import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf

import scopt.OptionParser
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

	def getValueFromRow[T](columnName: String, row: org.apache.spark.sql.Row): Option[T] = {
		Option(row.getAs[T](columnName))
	}

	def getApplicantNameTokens(row: org.apache.spark.sql.Row): Vector[String] = {
		val commonSuffixes = Vector("corp", "inc", "llc", "co", "ltd", "gmbh")
		val text = getValueFromRow[String]("applicant", row).map(_.toLowerCase)
		val tokens = text match {
			case Some(t) => NGramsLibSimple.tokens(t)
			case None => Vector[String]()
    }
    if (commonSuffixes contains tokens.last.replace(".", "")) tokens.slice(0, tokens.size - 1) else tokens
  }

  def getApplicantPostalCode(row: org.apache.spark.sql.Row): Option[String] = {
    getValueFromRow[String]("countrycode", row) match {
      case Some("US") => getValueFromRow[String]("zipcode", row).map(_.take(5))
      case _ => getValueFromRow[String]("postalcode", row)
    }
  }

  def getApplicantAddressTokens(row: org.apache.spark.sql.Row): Vector[String] = {
    val addressComponents = for (col <- Vector("street1", "street2", "city")) yield getValueFromRow[String](col, row)
    val addressComponentsAll = addressComponents :+ getApplicantPostalCode(row)
    val x = addressComponentsAll.flatMap(_.map(_.toLowerCase.replace("[^a-z0-9]", "")))
    x.flatMap(NGramsLibSimple.tokens)
  }

  case class ApplicantNGrams(name: Set[Vector[String]], address: Set[Vector[String]], state: Option[String], country: Option[String])

  def getApplicantNGrams(row: org.apache.spark.sql.Row): ApplicantNGrams = {
    val nGramsOrders = Set(1, 2)
    val nameNGrams = NGramsLibSimple.uniqueNGrams(getApplicantNameTokens(row), nGramsOrders)
    val addressNGrams = NGramsLibSimple.uniqueNGrams(getApplicantAddressTokens(row), nGramsOrders)
    val state = getValueFromRow[String]("state", row)
    val country = getValueFromRow[String]("countrycode", row)
    ApplicantNGrams(nameNGrams, addressNGrams, state, country)
  }

  def ApplicantTokenMap(df: org.apache.spark.sql.DataFrame): Map[Int, ApplicantNGrams] = {

    df.map(row => (row.getAs[Int]("applicantid"), getApplicantNGrams(row))).collect().toMap
  }

  val parser = new OptionParser[Config]("App") {

    head("App", "")
    opt[String]('d', "database") required() action { (x, c) =>
      c.copy(database = x)
    } text ("JDBC database URL (required)")
  }

  case class Config(database: String = "")
}

