/**
  * Created by JoseM on 2/20/2016.
  */
object NGramsLibSimple extends NGramLib {
  def tokens(text: String): Vector[String] = {
    text.split("\\s+").toVector
  }

  def nGrams(tokens: Vector[String], n: Int): Vector[Vector[String]] = {
    tokens.sliding(n, 1).toVector
  }

  def uniqueNGrams(tokens: Vector[String], n: Int): Set[Vector[String]] = {
    tokens.sliding(n, 1).toSet
  }

  def uniqueNGrams(tokens: Vector[String], nvals: Set[Int]): Set[Vector[String]] = {

    val ngrams = for (n <- nvals) yield tokens.sliding(n, 1).toSet
    ngrams.reduceLeft(_ union _)
  }

  def diceCoefficient(s1: Set[Vector[String]], s2: Set[Vector[String]]): Double = {
    2.0 * (s1 intersect s2).size / (s1.size + s2.size)
  }
}
