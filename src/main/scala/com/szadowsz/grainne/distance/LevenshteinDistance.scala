package com.szadowsz.grainne.distance

object LevenshteinDistance {

  private def distance(first: CharSequence, second: CharSequence): Int = {
    var previous: Array[Int] = Array.range(0, first.length + 1)
    var current: Array[Int] = new Array[Int](first.length + 1)
    var tmp: Array[Int] = Array()

    for (j <- 1 to first.length) {
      val next = second.charAt(j - 1)
      current(0) = j

      for (i <- 1 to second.length) {
        val cost = if (first.charAt(i - 1) == next) 0 else 1
        // minimum of cell to the left+1, to the top+1, diagonally left and up +cost
        current(i) = Math.min(Math.min(current(i - 1) + 1, previous(i) + 1), previous(i - 1) + cost)
      }

      // copy current distance counts to 'previous row' distance counts
      tmp = previous
      previous = current
      current = tmp
    }
    previous(first.length)
  }

  /**
   * Method to find the Levenshtein distance between two Strings.
   *
   * @param first - the first CharSequence to check, must not be null
   * @param second - the second CharSequence to check, must not be null
   * @throws IllegalArgumentException if either String input { @code null}
   */
  def difference(first: CharSequence, second: CharSequence): Int = {
    if (first == null || second == null) {
      throw new IllegalArgumentException("Arguments must not be null")

    } else if (first.length() == 0 || second.length() == 0) {
      Math.max(first.length(), second.length())

      // swap the input strings to consume less memory
    } else if (first.length() > second.length()) {
      distance(second, first)

    } else {
      distance(first,second)
    }
  }
}