/**
  * Created by JoseM on 2/20/2016.
  */
trait NGramLib {
  def tokens(text: String): Vector[String]
  def nGrams(tokens: Vector[String], n: Int): Vector[Vector[String]]
  def uniqueNGrams(tokens: Vector[String], n: Int): Set[Vector[String]]
  def uniqueNGrams(tokens: Vector[String], nvals: Set[Int]): Set[Vector[String]]
  def diceCoefficient(s1: Set[Vector[String]], s2: Set[Vector[String]]): Double
}
