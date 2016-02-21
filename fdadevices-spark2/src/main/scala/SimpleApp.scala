/**
  * Created by JoseM on 2/20/2016.
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scopt.OptionParser

import scala.collection.JavaConverters._

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

    val applicantNGrams = df.map(row => getApplicantNGrams(row)).collect().groupBy(_.zone)

    //val indexPairs = sc.parallelize(applicantNGrams.values.toSeq, 6).flatMap(getIndexPairs).collect()
    val indexPairs = applicantNGrams.values.toSeq.flatMap(getIndexPairs)
    println(indexPairs.head)

    val partSizes = applicantNGrams.keys.zip(applicantNGrams.values.map(_.size)).toMap
    val numPairs = applicantNGrams.values.map(x => (0.5 * x.size * (x.size - 1))).reduceLeft(_ + _)

    println(s"Pairs: $numPairs")
    println(s"Sizes (min, max) = (${partSizes.values.min}, ${partSizes.values.max})")
    println(partSizes)

    val caUS = Zone(Option("CA"), Option("US"))

    for (i <- 1 to 10) {

      println(applicantNGrams(caUS)(i))
    }
  }

  def getIndexPairs(ngrams: Array[ApplicantNGrams]) = {
    val pairs = (0 until ngrams.size).toSet.subsets(2)
    for (p <- pairs) yield (ngrams(p.head).applicantid, ngrams(p.last).applicantid)
  }

  case class ApplicantSimilarity(applicantId1: Int, applicantId2: Int, similarity: Double)

  def computeSimilarity(nGrams1: ApplicantNGrams, nGrams2: ApplicantNGrams): ApplicantSimilarity = {
    val diceName = NGramsLibSimple.diceCoefficient(nGrams1.name, nGrams2.name)
    val diceAddress = NGramsLibSimple.diceCoefficient(nGrams1.address, nGrams2.address)
    val meanDice = 0.5 * diceName + 0.5 * diceAddress
    ApplicantSimilarity(applicantId1=nGrams1.applicantid, applicantId2=nGrams2.applicantid, similarity=meanDice)
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

  case class Zone(state: Option[String], country: Option[String])
  case class ApplicantNGrams(applicantid: Int, name: Set[Vector[String]], address: Set[Vector[String]], zone: Zone)

  def getApplicantNGrams(row: org.apache.spark.sql.Row): ApplicantNGrams = {
    val nGramsOrders = Set(1, 2)
    val nameNGrams = NGramsLibSimple.uniqueNGrams(getApplicantNameTokens(row), nGramsOrders)
    val addressNGrams = NGramsLibSimple.uniqueNGrams(getApplicantAddressTokens(row), nGramsOrders)
    val id = row.getAs[Int]("applicantid")
    val state = getValueFromRow[String]("state", row)
    val country = getValueFromRow[String]("countrycode", row)
    ApplicantNGrams(id, nameNGrams, addressNGrams, Zone(state, country))
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
