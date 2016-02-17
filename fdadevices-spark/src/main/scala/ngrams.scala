
package fdadevices.nlp

import scala.collection.immutable

trait NGramsLib {
    def tokens(text: String): List[String]
    def nGrams(tokens: List[String], n: Int): List[List[String]]
    def uniqueNGrams(tokens: List[String], n: Int): Set[List[String]]
    def diceCoefficient(s1: Set[List[String]], s2: Set[List[String]]): Double
}

object NGramsLibSimple extends NGramsLib {

    def tokens(text: String): List[String] = {
	text.split("\\s+").toList
    }

    def nGrams(tokens: List[String], n: Int): List[List[String]] = {
	tokens.sliding(n, 1).toList
    }

    def uniqueNGrams(tokens: List[String], n: Int): Set[List[String]] = {
	tokens.sliding(n, 1).toSet
    }

    def diceCoefficient(s1: Set[List[String]], s2: Set[List[String]]): Double = {
	2.0 * (s1 intersect s2).size / (s1.size + s2.size)
    }
}
