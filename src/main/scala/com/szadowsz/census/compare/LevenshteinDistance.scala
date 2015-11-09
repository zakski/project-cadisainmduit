package com.szadowsz.census.compare

class LevenshteinDistance {
  /**
   * Method to find the Levenshtein distance between two Strings.
   *
   * @param first - the first CharSequence to check, must not be null
   * @param second - the second CharSequence to check, must not be null
   * @throws IllegalArgumentException if either String input { @code null}
   */
  def difference(first: CharSequence, second: CharSequence): Float = {
    if (first == null || second == null) {
      throw new IllegalArgumentException("Arguments must not be null")

    } else if (first.length() == 0 || second.length() == 0) {
      Math.max(first.length(), second.length())

      // swap the input strings to consume less memory
    } else if (first.length() > second.length()) {
      difference(second, first)

    } else {
      val n: Int = first.length
      val m: Int = second.length

      var previous: Array[Int] = Array.range(0, n + 1)
      var current: Array[Int] = new Array[Int](n + 1)
      var tmp: Array[Int] = Array()

      for (j <- 1 to m) {
        val next = second.charAt(j - 1)
        current(0) = j

        for (i <- 1 to n) {
          val cost = if (first.charAt(i - 1) == next) 0 else 1
          // minimum of cell to the left+1, to the top+1, diagonally left and up +cost
          current(i) = Math.min(Math.min(current(i - 1) + 1, previous(i) + 1), previous(i - 1) + cost)
        }

        // copy current distance counts to 'previous row' distance counts
        tmp = previous
        previous = current
        current = tmp
      }
      previous(n)
    }
  }
}