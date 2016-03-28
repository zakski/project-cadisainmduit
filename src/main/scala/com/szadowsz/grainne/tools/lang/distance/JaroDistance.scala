package com.szadowsz.grainne.tools.distance

/**
  * Created by zakski on 23/11/2015.
  */
object JaroDistance  {


  private def getShortAndLong(first: CharSequence, second: CharSequence): (String, String) = {
    if (first.length < second.length) {
      (first.toString.toLowerCase, second.toString.toLowerCase)
    } else {
      (second.toString.toLowerCase, first.toString.toLowerCase)
    }
  }

  /**
    * Method to find the Jaro distance between two Strings.
    *
    * @param first - the first CharSequence to check, must not be null
    * @param second - the second CharSequence to check, must not be null
    * @throws IllegalArgumentException if either String input { @code null}
    */
  def distance(first: CharSequence, second: CharSequence): Double = {
    if (first == null || second == null) {
      throw new IllegalArgumentException("Arguments must not be null")
    }

    val (short, long) = getShortAndLong(first, second)
    val halfLength: Int = long.length / 2 - 1

    val m1: String = getCommonCharacters(short, long, halfLength)
    val m2: String = getCommonCharacters(long, short, halfLength)

    if (m1.length == 0 || m2.length == 0 || m1.length != m2.length) {
      0.0
    } else {
      val transpositions: Int = getTranspositions(m1, m2)
      val defaultDenominator: Double = 3.0
      val dist: Double = (m1.length / (short.length.toDouble) + m2.length / (long.length.toDouble) + (m1.length - transpositions) / (m1.length.toDouble)) / defaultDenominator
      dist
    }
  }

  /**
    * Calculates the number of transposition between two strings.
    *
    * @param first The first string.
    * @param second The second string.
    * @return The number of transposition between the two strings.
    */
  protected def getTranspositions(first: String, second: String): Int = {
    first.zip(second).count(Function.tupled(_ != _)) / 2
  }

  /**
    * returns a string buffer of characters from string1 within string2 if they are of a given
    * distance seperation from the position in string1.
    *
    * @param string1
    * @param string2
    * @param distanceSep
    * @return a string buffer of characters from string1 within string2 if they are of a given
    *         distance seperation from the position in string1
    */
  private def getCommonCharacters(string1: String, string2: String, distanceSep: Int): String = {
    val returnCommons = new StringBuilder()
    val copy = new StringBuilder(string2)

    var i = 0
    for (string1Char <- string1) {
      var foundIt = false
      var j = math.max(0, i - distanceSep)
      while (!foundIt && j < math.min(i + distanceSep + 1, string2.length)) {
        if (copy.charAt(j) == string1Char) {
          foundIt = true
          returnCommons.append(string1Char)
          copy.setCharAt(j, 0.toChar)
        }
        j += 1
      }
      i += 1
    }

    returnCommons.toString
  }
}
