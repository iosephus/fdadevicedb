/**
  * Created by JoseM on 2/20/2016.
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scopt.OptionParser

object SimpleApp {
  def main(args: Array[String]) {
    val config = parser.parse(args, Config())

    if (!config.isDefined)
      System.exit(1)

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("FDADevices")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val connInfo = Map("url" -> config.get.database, "dbtable" -> "applicant")
    println("Reading data")
    val df = time { sqlContext.load("jdbc", connInfo) }


    println("Partitioning by zone")
    val applicantNGrams = time { df.map(row => getApplicantNGrams(row)).collect().groupBy(_.zone)}

    //val indexPairs = sc.parallelize(applicantNGrams.values.toSeq, 6).flatMap(getIndexPairs).collect()
    println("Computing index pairs")
    val arrSizes = applicantNGrams.values.map(_.size).toVector
    val groupOffsets = arrSizes.slice(0, arrSizes.size - 1).scanLeft(0)(_ + _)
    println("Array sizes")
    println(arrSizes)
    println("Group offsets")
    println(groupOffsets)
    //val indexPairs = time { sc.parallelize(arrSizes.zip(groupOffsets), 64).flatMap({case (size, offset) => getIndexPairs(size, offset)}).collect() }
    val indexPairs = time { arrSizes.zip(groupOffsets).flatMap({case (size, offset) => getIndexPairs(size, offset)}) }

    val flatApplicantNGrams = sc.broadcast(applicantNGrams.values.flatten.toVector)
    println(s"flatApplicantNGrams size: ${flatApplicantNGrams.value.size}")
    println(flatApplicantNGrams.value(0))
    println("Computing similarities")
    //val similarities = sc.parallelize(indexPairs.toSeq, 6).map(t => computeSimilarity(flatApplicantNGrams(t._1), flatApplicantNGrams(t._2))).collect()
    val similarities = time { sc.parallelize(indexPairs.toSeq, 256).flatMap(p => computeSimilarity(flatApplicantNGrams, p)).collect() }

    println(s"Similarity values: ${similarities.size}")

    val applicants: RDD[(VertexId, Int)] = sc.parallelize(for (ng <- flatApplicantNGrams.value) yield (ng.applicantid.toLong, ng.applicantid))
    val relationships: RDD[Edge[String]] = sc.parallelize(for (ng <- similarities if ng.similarity >= 0.7) yield Edge(ng.applicantId1, ng.applicantId2, "similarity"))

    val graph = Graph(applicants, relationships)

    val components = graph.connectedComponents()

    val graphMap = graph.vertices.leftJoin(components.vertices) { case (id, applicant, compid) => compid }.collect().groupBy(_._2)

    println(s"Connected components: ${graphMap.size}")

  }

  def getIndexPairs(size: Int, offset: Int = 0): Vector[(Int, Int)] = {
    val pairs = (offset until offset + size).toSet.subsets(2)
    (for (p <- pairs) yield (p.head, p.last)).toVector
  }

  case class ApplicantSimilarity(applicantId1: Int, applicantId2: Int, similarity: Double)

  def computeSimilarity(nGrams: Broadcast[Vector[ApplicantNGrams]], indexPair: (Int, Int), nameThreshold: Double = 0.1, addressThreshold: Double = 0.1): Option[ApplicantSimilarity] = {
    val nGramsLocal = nGrams.value
    val (index1, index2) = indexPair
    val nGrams1 = nGramsLocal(index1)
    val nGrams2 = nGramsLocal(index2)
    val diceName = NGramsLibSimple.diceCoefficient(nGrams1.name, nGrams2.name)
    if (diceName <= nameThreshold) {
      None
    } else {
      val diceAddress = NGramsLibSimple.diceCoefficient(nGrams1.address, nGrams2.address)
      if (diceAddress <= addressThreshold) {
        None
      } else {
        val meanDice = 0.5 * diceName + 0.5 * diceAddress
        Some(ApplicantSimilarity(applicantId1 = nGrams1.applicantid, applicantId2 = nGrams2.applicantid, similarity = meanDice))
      }
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

  def time[A](f: => A) = {
    val startTime = System.currentTimeMillis
    val ret = f
    val durationSeconds = (System.currentTimeMillis - startTime) / 1000
    println(s"Took $durationSeconds seconds")
    ret
  }

  case class Config(database: String = "")
}
