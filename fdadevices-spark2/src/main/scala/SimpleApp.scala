/**
  * Created by JoseM on 2/20/2016.
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SaveMode, Row, SQLContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types._
import math._

import scopt.OptionParser

object SimpleApp {
  def main(args: Array[String]) {
    val config = parser.parse(args, Config())

    if (!config.isDefined)
      System.exit(1)

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val prop = new java.util.Properties
    prop.setProperty("user", config.get.user)
    prop.setProperty("password", config.get.password)

    val conf = new SparkConf().setAppName("FDADevices")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    println("Reading data from database")
    val applicantDF = time { sqlContext.read.jdbc(config.get.database, "applicant", prop) }
    println(s"Number of applicants in database: ${applicantDF.count()}")

    println("Grouping by zone")
    val applicantNGrams = time { applicantDF.map(row => getApplicantNGrams(row)).collect().groupBy(_.zone)}
    println(s"${applicantNGrams.size} zones found")

    //val indexPairs = sc.parallelize(applicantNGrams.values.toSeq, 6).flatMap(getIndexPairs).collect()
    println("Computing index pairs")
    val arrSizes = applicantNGrams.values.map(_.size).toVector
    val groupOffsets = arrSizes.slice(0, arrSizes.size - 1).scanLeft(0)(_ + _)
    val indexPairs = time { arrSizes.zip(groupOffsets).flatMap({case (size, offset) => getIndexPairs(size, offset)}) }
    println(s"${indexPairs.size} index pairs generated")

    val flatApplicantNGrams = sc.broadcast(applicantNGrams.values.flatten.toVector)
    println("Computing similarities")
    //val similarities = sc.parallelize(indexPairs.toSeq, 6).map(t => computeSimilarity(flatApplicantNGrams(t._1), flatApplicantNGrams(t._2))).collect()
    val similarities = time { sc.parallelize(indexPairs.toSeq, min(indexPairs.size, 1024)).flatMap(p => computeSimilarity(flatApplicantNGrams, p)).collect() }
    //val similarities = time { indexPairs.flatMap(p => computeSimilarity(flatApplicantNGrams, p)) }

    println(s"Valid similarity values: ${similarities.size}")

    val applicants: RDD[(VertexId, Long)] = sc.parallelize(for (ng <- flatApplicantNGrams.value) yield (ng.applicantId.toLong, ng.applicantId), 12)
    val relationships: RDD[Edge[(String, Double)]] = sc.parallelize(for (ng <- similarities if ng.similarity >= 0.7) yield Edge(ng.applicantId1, ng.applicantId2, ("NameAddressNGramsMeanDice", ng.similarity)), 12)

    val similaritySchema = new StructType(Array(StructField("applicantid1", LongType, nullable=false), StructField("applicantid2", LongType, nullable=false), StructField("stype", StringType, nullable=false), StructField("value", DoubleType, nullable=false)))
    val similarityRDD: RDD[Row] = sc.parallelize(for (ng <- similarities) yield Row(ng.applicantId1, ng.applicantId2, "NameAddressNGramsMeanDice", ng.similarity), 12)
    val similarityDF = sqlContext.createDataFrame(similarityRDD, similaritySchema)

    println("Writing similarity values to database")
    time { similarityDF.write.mode("Overwrite").jdbc(config.get.database, "applicant_similarity", prop) }

    val graph = Graph(applicants, relationships)

    println("Computing connected components")
    val components = time { graph.connectedComponents() }

    val componentMap = components.vertices.collect().groupBy({ case (vertex, comp) => comp })
    println(componentMap)

    println(s"Connected components: ${componentMap.size}")

    val connectedComponentSchema =  new StructType(Array(StructField("applicantid", LongType, nullable=false), StructField("uapplicantid", LongType, nullable=false)))
    val connectedComponentsDF = sqlContext.createDataFrame(for ((vertex, comp) <- components.vertices) yield Row(vertex.toLong, comp.toLong), connectedComponentSchema)

    val applicantDF2 = applicantDF.join(connectedComponentsDF, "applicantid")
    val uapplicantDF = applicantDF2.drop("applicantid").dropDuplicates(Array("uapplicantid"))

    time { uapplicantDF.write.mode("Overwrite").jdbc(config.get.database, "uapplicant", prop) }
    time { applicantDF2.write.mode("Overwrite").jdbc(config.get.database, "applicant2", prop) }

    /* TODO!!!
    val contactDF = sqlContext.read.jdbc(config.get.database, "contact", prop).withColumnRenamed("id", "applicantid_toremove")
    val contactDF2 = contactDF.join(connectedComponentsDF, contactDF("applicantid_toremove") === connectedComponentsDF("applicantid"), "left").drop("applicantid_toremove")
    val uContactDF = contactDF2.dropDuplicates(Array("contact", "uapplicantid")).withColumnRenamed("contactid", "ucontactid")
    val contactDF3 = contactDF2.join(uContactDF, contactDF2("contact") === uContactDF("contact") && contactDF2("uapplicantid") === uContactDF("uapplicantid"), "left")
    */

    val pmnRequestDF = sqlContext.read.jdbc(config.get.database, "pmn_request", prop).withColumnRenamed("applicantid", "applicantid_toremove")
    val pmnRequestDF2 = pmnRequestDF.join(connectedComponentsDF, pmnRequestDF("applicantid_toremove") === connectedComponentsDF("applicantid"), "left").drop("applicantid_toremove")
    time { pmnRequestDF2.write.mode("Overwrite").jdbc(config.get.database, "pmn_request2", prop) }
  }

  case class Config(database: String = "", user: String = "", password: String = "")
  case class ApplicantSimilarity(applicantId1: Long, applicantId2: Long, similarity: Double)
  case class Zone(state: Option[String], country: Option[String])
  case class ApplicantNGrams(applicantId: Long, name: Set[Vector[String]], address: Set[Vector[String]], zone: Zone)

  def getIndexPairs(size: Int, offset: Int = 0): Vector[(Int, Int)] = {
    val pairs = (offset until offset + size).toSet.subsets(2)
    (for (p <- pairs) yield (p.head, p.last)).toVector
  }

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
        Some(ApplicantSimilarity(applicantId1 = nGrams1.applicantId, applicantId2 = nGrams2.applicantId, similarity = meanDice))
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

  def time[A](f: => A) = {
    val startTime = System.currentTimeMillis
    val ret = f
    val durationSeconds = (System.currentTimeMillis - startTime) / 1000
    println(s"Took $durationSeconds seconds")
    ret
  }

  val parser = new OptionParser[Config]("App") {

    head("App", "")
    opt[String]('d', "database") required() action { (x, c) =>
      c.copy(database = x)
    } text ("JDBC database URL (required)")
    opt[String]('u', "user") required() action { (x, c) =>
      c.copy(user = x)
    } text ("JDBC database user (required)")
    opt[String]('p', "password") required() action { (x, c) =>
      c.copy(user = x)
    } text ("JDBC database password (required)")
  }

}
